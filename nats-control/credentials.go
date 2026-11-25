package control

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const credentialLifetime = 24 * time.Hour

var tenantID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var machineName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type credentials struct {
	JWT       string    `json:"jwt"`
	Seed      string    `json:"seed"`
	ExpiresAt time.Time `json:"expires_at"`
}

func agentInboxPrefix(tenant, name string) string {
	return "_INBOX.csl." + tenant + "." + name
}

func isRegisteredName(name string) bool {
	return agentName.MatchString(name) && name != "owner" && name != "csl-control" && name != "csl-ci"
}

func validAgent(agent Agent) bool {
	return (agent.Role == "worker" || agent.Role == "orchestrator") &&
		(agent.CLI == "claude" || agent.CLI == "codex") && machineName.MatchString(agent.Machine)
}

func issueCredentials(signer nkeys.KeyPair, account, tenant, name string, agent Agent, now time.Time, domains ...string) (credentials, string, error) {
	if signer == nil || !nkeys.IsValidPublicAccountKey(account) || !tenantID.MatchString(tenant) ||
		!isRegisteredName(name) || !validAgent(agent) {
		return credentials{}, "", errors.New("invalid tenant, signing account, or agent registration")
	}
	signerKey, err := signer.PublicKey()
	if err != nil || !nkeys.IsValidPublicAccountKey(signerKey) {
		return credentials{}, "", errors.New("invalid account signer")
	}
	user, err := nkeys.CreateUser()
	if err != nil {
		return credentials{}, "", err
	}
	defer user.Wipe()
	publicKey, err := user.PublicKey()
	if err != nil {
		return credentials{}, "", err
	}
	seed, err := user.Seed()
	if err != nil {
		return credentials{}, "", err
	}
	defer clear(seed)

	root := "csl." + tenant
	consumer := "CSL_MSG.agent-" + name
	wake := root + ".agent." + name + ".wake"
	claims := jwt.NewUserClaims(publicKey)
	claims.Name = name
	if signerKey != account {
		claims.IssuerAccount = account
	}
	claims.Expires = now.Add(credentialLifetime).Unix()
	claims.Tags = jwt.TagList{"role:" + agent.Role, "cli:" + agent.CLI, "machine:" + agent.Machine}
	claims.Pub.Allow = jwt.StringList{
		root + ".send." + name,
		root + ".ctl.*." + name,
		"$JS.API.CONSUMER.INFO." + consumer,
		"$JS.API.CONSUMER.MSG.NEXT." + consumer,
		"$JS.ACK." + consumer + ".>",
	}
	for _, domain := range domains {
		if !machineName.MatchString(domain) || strings.Contains(domain, ".") {
			return credentials{}, "", errors.New("invalid JetStream domain")
		}
		claims.Pub.Allow = append(claims.Pub.Allow,
			"$JS."+domain+".API.CONSUMER.INFO."+consumer,
			"$JS."+domain+".API.CONSUMER.MSG.NEXT."+consumer,
			"$JS.ACK."+domain+".*."+consumer+".>",
		)
	}
	claims.Sub.Allow = jwt.StringList{wake, agentInboxPrefix(tenant, name) + ".>"}

	token, err := claims.Encode(signer)
	if err != nil {
		return credentials{}, "", err
	}
	return credentials{JWT: token, Seed: string(seed), ExpiresAt: time.Unix(claims.Expires, 0).UTC()}, publicKey, nil
}
