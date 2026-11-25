package control

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	pairIdleLimit    = 3 * time.Hour
	reminderInterval = 20 * time.Minute
	recentWrite      = 10 * time.Minute
)

var agentName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}$`)
var buildVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

// Agent is the record stored at agent.<name> in CSL_STATE.
type Agent struct {
	Role          string    `json:"role"`
	CLI           string    `json:"cli"`
	Machine       string    `json:"machine"`
	Delivery      string    `json:"delivery"`
	ClientVersion string    `json:"client_version"`
	LastSeen      time.Time `json:"last_seen"`
	Status        string    `json:"status"`
	TaskComplete  bool      `json:"task_complete"`
}

func defaultDelivery(role string) string {
	if role == "orchestrator" {
		return "grouped:15"
	}
	return "immediate"
}

func deliveryInterval(delivery string) (time.Duration, error) {
	if delivery == "immediate" {
		return 0, nil
	}
	minutes, ok := strings.CutPrefix(delivery, "grouped:")
	n, err := strconv.ParseUint(minutes, 10, 64)
	if !ok || err != nil || n == 0 || strconv.FormatUint(n, 10) != minutes ||
		n > uint64(time.Duration(1<<63-1)/time.Minute) {
		return 0, errors.New("delivery must be immediate or grouped:<positive integer minutes>")
	}
	return time.Duration(n) * time.Minute, nil
}

// PairSession is the record stored at session.<a>.<b> in CSL_STATE.
type PairSession struct {
	OpenedAt  time.Time `json:"opened_at"`
	LastMsgAt time.Time `json:"last_msg_at"`
	LastFrom  string    `json:"last_from"`
	Closed    bool      `json:"closed"`
}

func pairKey(a, b string) (string, error) {
	if a == b || !agentName.MatchString(a) || !agentName.MatchString(b) {
		return "", errors.New("a pair requires two distinct agent names")
	}
	if a > b {
		a, b = b, a
	}
	return "session." + a + "." + b, nil
}

func (s PairSession) isOpen(now time.Time) bool {
	return !s.Closed && !s.OpenedAt.IsZero() && now.Before(s.LastMsgAt.Add(pairIdleLimit))
}

func (s PairSession) mention(sender string, at time.Time) PairSession {
	// A delayed or duplicate observation cannot move the activity watermark or
	// reopen an explicitly closed session.
	if !at.After(s.LastMsgAt) {
		return s
	}
	at = at.UTC()
	if !s.isOpen(at) {
		s = PairSession{OpenedAt: at}
	}
	s.LastMsgAt, s.LastFrom = at, sender
	return s
}

type reminderSession struct {
	peer       string
	session    PairSession
	lastSentAt time.Time
}

func reminderBody(agent Agent, sessions []reminderSession, now time.Time) string {
	if agent.Role != "worker" {
		return ""
	}
	var peers []string
	for _, entry := range sessions {
		if entry.session.isOpen(now) && !entry.lastSentAt.After(now.Add(-recentWrite)) {
			peers = append(peers, entry.peer)
		}
	}
	if len(peers) == 0 {
		return ""
	}
	slices.Sort(peers)
	return "Open sessions: " + strings.Join(peers, ", ") +
		". If you owe an update, send it now. Do not reply to this reminder."
}

// VersionPolicy is the record stored at policy.versions in CSL_STATE.
type VersionPolicy struct {
	Minimum     string `json:"minimum"`
	Recommended string `json:"recommended"`
}

func compareVersion(a, b string) (int, error) {
	first, second := buildVersion.FindStringSubmatch(a), buildVersion.FindStringSubmatch(b)
	if first == nil || second == nil {
		return 0, errors.New("expected a semantic release version")
	}
	// Compare decimal components by length first. This avoids integer overflow
	// and ignores the build identity, which does not affect release precedence.
	for i := 1; i <= 3; i++ {
		if len(first[i]) < len(second[i]) {
			return -1, nil
		}
		if len(first[i]) > len(second[i]) {
			return 1, nil
		}
		if order := strings.Compare(first[i], second[i]); order != 0 {
			return order, nil
		}
	}
	return 0, nil
}

func (p VersionPolicy) check(client string) (outdated, warn bool, err error) {
	if order, err := compareVersion(p.Minimum, p.Recommended); err != nil || order > 0 {
		return false, false, errors.New("invalid version policy")
	}
	minimum, err := compareVersion(client, p.Minimum)
	if err != nil {
		return false, false, err
	}
	recommended, _ := compareVersion(client, p.Recommended)
	return minimum < 0, minimum >= 0 && recommended < 0, nil
}
