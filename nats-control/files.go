package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const maxFile = 25 * 1024 * 1024
const maxChunk = 256 * 1024
const uploadTTL = time.Hour

type fileInfo struct {
	Name    string    `json:"name"`
	Size    int64     `json:"size"`
	From    string    `json:"from"`
	Allowed []string  `json:"allowed"`
	Created time.Time `json:"created_at"`
}

type fileRequest struct {
	ID      string   `json:"id"`
	Name    string   `json:"name,omitempty"`
	Size    int64    `json:"size,omitempty"`
	Allowed []string `json:"allowed,omitempty"`
	Offset  int64    `json:"offset,omitempty"`
	Data    []byte   `json:"data,omitempty"`
}

type fileResult struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	Data       []byte `json:"data,omitempty"`
	NextOffset *int64 `json:"next_offset"`
}

type uploadRecord struct {
	File    fileInfo  `json:"file"`
	Lease   string    `json:"lease"`
	Expires time.Time `json:"expires"`
	Offsets []int64   `json:"offsets"`
	Next    int64     `json:"next"`
}

func (s *Service) file(ctx context.Context, caller, verb string, body []byte) (any, error) {
	var request fileRequest
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	if !fileID.MatchString(request.ID) || request.Offset < 0 {
		return nil, invalid("invalid file ID or offset")
	}
	info, err := s.files.GetInfo(ctx, request.ID)
	var existing *fileInfo
	switch {
	case err == nil:
		meta, err := readFileInfo(info)
		if err != nil {
			return nil, err
		}
		existing = &meta
	case !errors.Is(err, jetstream.ErrObjectNotFound):
		return nil, err
	}
	switch verb {
	case "put":
		return s.putChunk(ctx, caller, request, existing)
	case "fetch":
		if existing == nil || !existing.allows(caller) {
			return nil, missing("file unavailable")
		}
		return s.fetchChunk(ctx, request, *existing)
	case "file-acl":
		if existing == nil || (existing.From != caller && caller != "owner") {
			return nil, missing("file unavailable")
		}
		allowed, err := s.validateACL(ctx, request.Allowed)
		if err != nil {
			return nil, err
		}
		existing.Allowed = allowed
		return *existing, s.files.UpdateMeta(ctx, request.ID, objectMeta(request.ID, *existing))
	default:
		return nil, missing("unknown file operation")
	}
}

func (f fileInfo) allows(caller string) bool {
	return caller == f.From || caller == "owner" || slices.Contains(f.Allowed, caller)
}

func objectMeta(id string, file fileInfo) jetstream.ObjectMeta {
	allowed, _ := json.Marshal(file.Allowed)
	return jetstream.ObjectMeta{Name: id, Metadata: map[string]string{
		"name": file.Name, "size": strconv.FormatInt(file.Size, 10), "from": file.From,
		"allowed": string(allowed), "created_at": file.Created.Format(time.RFC3339Nano),
	}}
}

func readFileInfo(info *jetstream.ObjectInfo) (fileInfo, error) {
	f := fileInfo{Name: info.Metadata["name"], Size: int64(info.Size), From: info.Metadata["from"]}
	if err := json.Unmarshal([]byte(info.Metadata["allowed"]), &f.Allowed); err != nil {
		return fileInfo{}, errors.New("file ACL metadata is unavailable")
	}
	f.Created, _ = time.Parse(time.RFC3339Nano, info.Metadata["created_at"])
	if f.From == "" || f.Name == "" || f.Size < 0 || f.Size > maxFile {
		return fileInfo{}, errors.New("file metadata is invalid")
	}
	return f, nil
}

func (s *Service) validateACL(ctx context.Context, names []string) ([]string, error) {
	allowed := append([]string{}, names...)
	slices.Sort(allowed)
	allowed = slices.Compact(allowed)
	for _, name := range allowed {
		if !isRegisteredName(name) {
			return nil, invalid("invalid file recipient")
		}
		if _, _, err := readState[Agent](ctx, s.state, "agent."+name); err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return nil, missing("unknown file recipient")
			}
			return nil, err
		}
	}
	return allowed, nil
}

func (s *Service) putChunk(ctx context.Context, caller string, request fileRequest, existing *fileInfo) (fileResult, error) {
	policy, err := s.deliveryPolicy(ctx)
	if err != nil {
		return fileResult{}, err
	}
	if request.Name == "" || len(request.Name) > 255 || path.Base(request.Name) != request.Name ||
		strings.ContainsAny(request.Name, "\\\x00\r\n") || request.Name == "." || request.Name == ".." ||
		request.Size < 0 || request.Size > policy.FileBytes || len(request.Data) > maxChunk ||
		request.Offset > request.Size || int64(len(request.Data)) > request.Size-request.Offset ||
		(len(request.Data) == 0 && request.Size != 0) {
		return fileResult{}, invalid("invalid upload metadata or chunk size")
	}
	allowed, err := s.validateACL(ctx, request.Allowed)
	if err != nil {
		return fileResult{}, err
	}
	result := fileResult{ID: request.ID, Name: request.Name, Size: request.Size}
	if existing != nil {
		if existing.From != caller || existing.Name != request.Name || existing.Size != request.Size ||
			!slices.Equal(existing.Allowed, allowed) {
			return fileResult{}, forbidden("file ID already exists with different ownership or metadata")
		}
		return result, nil
	}
	key := "upload." + caller + "." + request.ID
	record, revision, err := readState[uploadRecord](ctx, s.state, key)
	now := s.clock().UTC()
	absent := errors.Is(err, jetstream.ErrKeyNotFound)
	if err != nil && !absent {
		return fileResult{}, err
	}
	if absent || !record.Expires.After(now) {
		if request.Offset != 0 {
			return fileResult{}, invalid("upload must start from offset zero")
		}
		record = uploadRecord{
			File:  fileInfo{request.Name, request.Size, caller, allowed, now},
			Lease: randomID(), Expires: now.Add(uploadTTL),
		}
	}
	if record.File.Name != request.Name || record.File.Size != request.Size || !slices.Equal(record.File.Allowed, allowed) {
		return fileResult{}, invalid("upload metadata changed")
	}
	remaining := record.Expires.Sub(s.clock())
	if remaining <= 0 || !messageID.MatchString(record.Lease) {
		return fileResult{}, invalid("upload expired; restart from offset zero")
	}
	remaining = max(time.Second, remaining)
	if request.Offset > record.Next {
		return fileResult{}, invalid("upload offset is out of order")
	}
	chunkRoot := key + "." + record.Lease + "."
	chunkKey := chunkRoot + strconv.FormatInt(request.Offset, 10)
	chunk, err := s.state.Get(ctx, chunkKey)
	switch {
	case err == nil && !bytes.Equal(chunk.Value(), request.Data):
		return fileResult{}, invalid("replayed chunk differs")
	case errors.Is(err, jetstream.ErrKeyNotFound) && request.Offset == record.Next:
		if _, err := s.state.Create(ctx, chunkKey, request.Data, jetstream.KeyTTL(remaining)); err != nil {
			return fileResult{}, err
		}
	case err != nil:
		return fileResult{}, err
	}
	if request.Offset == record.Next && (request.Size != 0 || len(record.Offsets) == 0) {
		record.Next += int64(len(request.Data))
		record.Offsets = append(record.Offsets, request.Offset)
		if _, err := s.writeExpiring(ctx, key, record, revision, remaining); err != nil {
			return fileResult{}, err
		}
	}
	if record.Next < request.Size {
		result.NextOffset = &record.Next
		return result, nil
	}
	var file bytes.Buffer
	file.Grow(int(record.File.Size))
	for _, offset := range record.Offsets {
		entry, err := s.state.Get(ctx, chunkRoot+strconv.FormatInt(offset, 10))
		if err != nil {
			return fileResult{}, err
		}
		_, _ = file.Write(entry.Value())
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(file.Bytes()))
	if int64(file.Len()) != record.File.Size || digest != request.ID {
		return fileResult{}, invalid("uploaded data does not match its SHA-256 ID or size")
	}
	if _, err := s.files.Put(ctx, objectMeta(request.ID, record.File), &file); err != nil {
		return fileResult{}, err
	}
	return result, nil
}

func (s *Service) fetchChunk(ctx context.Context, request fileRequest, info fileInfo) (fileResult, error) {
	if request.Offset > info.Size {
		return fileResult{}, invalid("file offset is out of range")
	}
	object, err := s.files.Get(ctx, request.ID)
	if err != nil {
		return fileResult{}, err
	}
	defer object.Close()
	if _, err := io.CopyN(io.Discard, object, request.Offset); err != nil {
		return fileResult{}, err
	}
	body, err := io.ReadAll(io.LimitReader(object, min(maxChunk, info.Size-request.Offset)))
	if err != nil {
		return fileResult{}, err
	}
	next := request.Offset + int64(len(body))
	if next != min(request.Offset+maxChunk, info.Size) {
		return fileResult{}, errors.New("truncated file chunk")
	}
	result := fileResult{ID: request.ID, Name: info.Name, Size: info.Size, Data: body}
	if next < info.Size {
		result.NextOffset = &next
	}
	return result, nil
}

func (s *Service) authorizeAttachments(ctx context.Context, envelope Envelope) error {
	for _, attachment := range envelope.Attachments {
		object, err := s.files.GetInfo(ctx, attachment.ID)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return missing("attachment unavailable")
		}
		if err != nil {
			return err
		}
		file, err := readFileInfo(object)
		if err != nil {
			return err
		}
		if !file.allows(envelope.From) || !file.allows(envelope.To) ||
			file.Name != attachment.Name || file.Size != attachment.Size {
			return forbidden("attachment metadata or access does not match the recipient")
		}
	}
	return nil
}

// Keep a file if any retained or unacknowledged message references it. An
// upload with no references still uses the Object Store's normal TTL.
func (s *Service) pruneFiles(ctx context.Context, entries []messageEntry) error {
	if !slices.ContainsFunc(entries, func(entry messageEntry) bool {
		return len(entry.record.Envelope.Attachments) != 0
	}) {
		return nil
	}
	retention, err := s.retention(ctx)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	retained := map[string]bool{}
	expired := map[string]time.Time{}
	for i := len(entries) - 1; i >= 0; i-- {
		record := entries[i].record
		env := record.Envelope
		if env.Kind != "message" && env.Kind != "reply" {
			continue
		}
		pair, _ := pairKey(env.From, env.To)
		counts[pair]++
		if len(env.Attachments) == 0 {
			continue
		}
		keep := counts[pair] <= retention
		if !keep {
			state, _, err := readState[messageState](ctx, s.state, "msg."+env.ID)
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			keep = state.State != "acked" && state.State != "dropped"
		}
		for _, attachment := range env.Attachments {
			if keep {
				retained[attachment.ID] = true
			} else if _, ok := expired[attachment.ID]; !ok {
				expired[attachment.ID] = record.AcceptedAt
			}
		}
	}
	for id, reference := range expired {
		if retained[id] {
			continue
		}
		object, err := s.files.GetInfo(ctx, id)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		file, err := readFileInfo(object)
		if err != nil {
			return err
		}
		if !file.Created.After(reference) {
			if err := s.files.Delete(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
