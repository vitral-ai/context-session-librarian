package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Provisioner runs beside the operator material, outside tenant control. The
// returned account identity is public. Seeds and credentials stay in Directory.
type Provisioner struct {
	Operator      nkeys.KeyPair
	System        *nats.Conn
	Directory     string
	ConnectTenant func(credentialsPath string) (*nats.Conn, error)
}

func (p Provisioner) Create(ctx context.Context, request TenantRequest) (TenantInfo, error) {
	request, err := normalizedTenant(request)
	if err != nil {
		return TenantInfo{}, err
	}
	if p.Operator == nil || p.System == nil || p.Directory == "" || p.ConnectTenant == nil {
		return TenantInfo{}, errors.New("operator provisioning is not configured")
	}
	dir := filepath.Join(p.Directory, request.Tenant)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return TenantInfo{}, err
	}
	config, err := json.Marshal(request)
	if err != nil {
		return TenantInfo{}, err
	}
	path := filepath.Join(dir, "tenant.json")
	if stored, err := os.ReadFile(path); err == nil {
		var previous TenantRequest
		if json.Unmarshal(stored, &previous) != nil || previous != request {
			return TenantInfo{}, invalid("tenant already exists with different quotas")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return TenantInfo{}, err
	} else if err := createPrivateFile(path, config); err != nil {
		return TenantInfo{}, err
	}
	key, err := loadOrCreateAccount(filepath.Join(dir, "account.nk"))
	if err != nil {
		return TenantInfo{}, err
	}
	defer key.Wipe()
	account, err := key.PublicKey()
	if err != nil {
		return TenantInfo{}, err
	}
	accountClaims, err := lookupAccount(ctx, p.System, account)
	if err != nil {
		return TenantInfo{}, err
	}
	if accountClaims == nil {
		accountClaims = jwt.NewAccountClaims(account)
		accountClaims.Name = request.Tenant
		accountClaims.Limits.DiskStorage = request.StorageBytes
		accountClaims.Limits.Streams = 3
		accountClaims.Limits.Consumer = -1
		accountClaims.Limits.Payload = 1024 * 1024
		if err := pushAccount(ctx, p.System, p.Operator, "$SYS.REQ.CLAIMS.UPDATE", accountClaims); err != nil {
			return TenantInfo{}, err
		}
	} else if accountClaims.Name != request.Tenant {
		return TenantInfo{}, invalid("resolver account does not match the tenant")
	}
	if err := writeTenantUser(filepath.Join(dir, "owner.creds"), key, "owner",
		[]string{"csl." + request.Tenant + ".ctl.admin.owner"},
		[]string{agentInboxPrefix(request.Tenant, "owner") + ".>"}); err != nil {
		return TenantInfo{}, err
	}
	creds := filepath.Join(dir, "csl-control.creds")
	if err := writeTenantUser(creds, key, "csl-control", nil, nil); err != nil {
		return TenantInfo{}, err
	}
	conn, err := p.ConnectTenant(creds)
	if err != nil {
		return TenantInfo{}, err
	}
	defer conn.Close()
	if err := provisionStores(ctx, conn, request); err != nil {
		return TenantInfo{}, err
	}
	return TenantInfo{Tenant: request.Tenant, Account: account}, nil
}

func lookupAccount(ctx context.Context, system *nats.Conn, account string) (*jwt.AccountClaims, error) {
	if !nkeys.IsValidPublicAccountKey(account) {
		return nil, invalid("invalid public account key")
	}
	reply, err := system.RequestWithContext(ctx, "$SYS.REQ.ACCOUNT."+account+".CLAIMS.LOOKUP", nil)
	if err != nil {
		return nil, err
	}
	if len(reply.Data) == 0 {
		return nil, nil
	}
	claims, err := jwt.DecodeAccountClaims(string(reply.Data))
	if err != nil || claims.Subject != account {
		return nil, errors.New("resolver returned the wrong account")
	}
	return claims, nil
}

// RevokeAccountUsers preserves the active account configuration and pushes only
// the added revocation claims. The caller keeps the operator seed and SYS creds.
func RevokeAccountUsers(ctx context.Context, system *nats.Conn, operator nkeys.KeyPair, account string, keys []string, at time.Time) error {
	return revokeAccountUsers(ctx, []*nats.Conn{system}, operator, account, keys, at)
}

// RevokeAccountUsersOnPair reconciles the two isolated account resolvers before
// declaring a batch complete. A retry starts from both current claims.
func RevokeAccountUsersOnPair(ctx context.Context, local, peer *nats.Conn, operator nkeys.KeyPair, account string, keys []string, at time.Time) error {
	localID, peerID := local.ConnectedServerId(), peer.ConnectedServerId()
	if localID == "" || peerID == "" || localID == peerID {
		return errors.New("revocation requires two distinct connected brokers")
	}
	return revokeAccountUsers(ctx, []*nats.Conn{local, peer}, operator, account, keys, at)
}

func revokeAccountUsers(ctx context.Context, resolvers []*nats.Conn, operator nkeys.KeyPair, account string, keys []string, at time.Time) error {
	for _, key := range keys {
		if !nkeys.IsValidPublicUserKey(key) {
			return invalid("invalid user revocation key")
		}
	}
	var claims *jwt.AccountClaims
	revocations := make(jwt.RevocationList)
	previous := make([]jwt.RevocationList, 0, len(resolvers))
	for i, resolver := range resolvers {
		current, err := lookupAccount(ctx, resolver, account)
		if err != nil {
			return fmt.Errorf("resolver %d lookup: %w", i+1, err)
		}
		if current == nil {
			return missing("tenant account is unavailable")
		}
		if claims == nil {
			claims = current
		} else if !sameAccountPolicy(claims, current) {
			return errors.New("account policy differs across resolvers")
		}
		previous = append(previous, current.Revocations)
		for key, cutoff := range current.Revocations {
			revocations.Revoke(key, time.Unix(cutoff, 0))
		}
	}
	for _, key := range keys {
		if !revocations.IsRevoked(key, at) {
			revocations.Revoke(key, at)
		}
	}
	changed := false
	for _, current := range previous {
		changed = changed || !maps.Equal(current, revocations)
	}
	if !changed {
		return nil
	}
	claims.Revocations = revocations
	token, err := claims.Encode(operator)
	if err != nil {
		return err
	}
	subject := "$SYS.REQ.ACCOUNT." + account + ".CLAIMS.UPDATE"
	var updateError error
	for i, resolver := range resolvers {
		if err := pushAccountToken(ctx, resolver, subject, account, token); err != nil {
			updateError = errors.Join(updateError, fmt.Errorf("resolver %d update: %w", i+1, err))
		}
	}
	if updateError != nil {
		return updateError
	}
	for i, resolver := range resolvers {
		current, err := lookupAccount(ctx, resolver, account)
		if err != nil {
			return fmt.Errorf("resolver %d readback: %w", i+1, err)
		}
		if current == nil || !sameAccountPolicy(claims, current) || !maps.Equal(revocations, current.Revocations) {
			return fmt.Errorf("resolver %d did not confirm the revocation claim", i+1)
		}
	}
	return nil
}

func sameAccountPolicy(a, b *jwt.AccountClaims) bool {
	left, right := *a, *b
	left.Revocations, right.Revocations = nil, nil
	left.ID, right.ID = "", ""
	left.IssuedAt, right.IssuedAt = 0, 0
	left.Issuer, right.Issuer = "", ""
	return reflect.DeepEqual(left, right)
}

func pushAccount(ctx context.Context, system *nats.Conn, signer nkeys.KeyPair, subject string, account *jwt.AccountClaims) error {
	token, err := account.Encode(signer)
	if err != nil {
		return err
	}
	return pushAccountToken(ctx, system, subject, account.Subject, token)
}

func pushAccountToken(ctx context.Context, system *nats.Conn, subject, account, token string) error {
	reply, err := system.RequestWithContext(ctx, subject, []byte(token))
	if err != nil {
		return err
	}
	var response struct {
		Data struct {
			Account string `json:"account"`
		} `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &response); err != nil ||
		(len(response.Error) != 0 && string(response.Error) != "null") || response.Data.Account != account {
		return errors.New("resolver rejected the tenant account update")
	}
	return nil
}

func loadOrCreateAccount(path string) (nkeys.KeyPair, error) {
	seed, err := os.ReadFile(path)
	if err == nil {
		defer clear(seed)
		return nkeys.FromSeed(seed)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, err := nkeys.CreateAccount()
	if err != nil {
		return nil, err
	}
	seed, err = key.Seed()
	if err != nil {
		key.Wipe()
		return nil, err
	}
	if err := createPrivateFile(path, seed); err != nil {
		key.Wipe()
		return nil, err
	}
	return key, nil
}

func writeTenantUser(path string, account nkeys.KeyPair, name string, pub, sub []string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	key, err := nkeys.CreateUser()
	if err != nil {
		return err
	}
	defer key.Wipe()
	public, err := key.PublicKey()
	if err != nil {
		return err
	}
	claims := jwt.NewUserClaims(public)
	claims.Name, claims.Pub.Allow, claims.Sub.Allow = name, pub, sub
	token, err := claims.Encode(account)
	if err != nil {
		return err
	}
	seed, err := key.Seed()
	if err != nil {
		return err
	}
	defer clear(seed)
	material, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		return err
	}
	defer clear(material)
	return createPrivateFile(path, material)
}

func createPrivateFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".csl-provision-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Publish complete files atomically without replacing another writer.
	if err := os.Link(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func provisionStores(ctx context.Context, conn *nats.Conn, request TenantRequest) error {
	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}
	partition := request.StorageBytes / 4
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "CSL_MSG", Subjects: []string{"csl." + request.Tenant + ".agent.*.inbox"},
		Storage: jetstream.FileStorage, MaxAge: messageAge, Duplicates: 2 * time.Minute,
		MaxMsgSize: 1024 * 1024, MaxBytes: 2 * partition,
	}); err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("create tenant messages: %w", err)
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "CSL_STATE", History: 1, MaxBytes: partition, LimitMarkerTTL: time.Minute,
	})
	if errors.Is(err, jetstream.ErrBucketExists) {
		kv, err = js.KeyValue(ctx, "CSL_STATE")
	}
	if err != nil {
		return fmt.Errorf("create tenant state: %w", err)
	}
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: "CSL_FILES", TTL: messageAge, MaxBytes: partition,
	}); err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("create tenant files: %w", err)
	}
	policy := tenantPolicy{Retention: defaultRetain, UrgentCap: 10,
		FileBytes: request.FileBytes, RatePerMinute: request.RatePerMinute}
	if err := writeState(ctx, kv, "policy.delivery", policy, 0); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	return nil
}
