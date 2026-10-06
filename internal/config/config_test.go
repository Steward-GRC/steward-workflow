// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/workloadauth"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func required() map[string]string {
	return map[string]string{
		"DATABASE_DSN":         "postgres://workflow:workflow@localhost:5432/workflow",
		"RABBITMQ_URL":         "amqp://guest:guest@localhost:5672/",
		"WORKLOAD_OIDC_ISSUER": "https://issuer.example.org", "WORKLOAD_ALLOWED_SERVICEACCOUNTS": "steward/steward-gateway",
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(required()))
	require.NoError(t, err)
	require.Equal(t, "9092", c.GRPCPort)
	require.Equal(t, "8080", c.ProbePort)
	require.Equal(t, "migrations", c.MigrationsDir)
	require.Equal(t, c.DatabaseDSN, c.SagaDatabaseDSN)
	require.Equal(t, c.DatabaseDSN, c.MigrateDSN)
	require.Equal(t, 72, c.StageSLAHours)
	require.Equal(t, 48, c.StageReminderHours)
	require.True(t, c.WorkloadAuthEnabled, "workload auth is on unless switched off")
	require.Equal(t, workloadauth.Config{
		Issuer: "https://issuer.example.org", Audience: "steward", AllowedServiceAccounts: []string{"steward/steward-gateway"},
	}, c.WorkloadAuth)
	require.Equal(t, "/var/run/secrets/steward/token", c.TokenFile, "with auth on, workflow sends its own token by default")
	require.Equal(t, "identity:9090", c.IdentityAddr)
	require.Equal(t, "core:9090", c.CoreAddr)
}

func TestLoadMissingRequired(t *testing.T) {
	for _, k := range []string{"DATABASE_DSN", "RABBITMQ_URL"} {
		m := required()
		delete(m, k)
		_, err := Load(env(m))
		require.ErrorContains(t, err, k)
	}
}

func TestLoadRejectsBadHours(t *testing.T) {
	m := required()
	m["STAGE_SLA_HOURS"] = "soon"
	_, err := Load(env(m))
	require.ErrorContains(t, err, "STAGE_SLA_HOURS")

	m = required()
	m["STAGE_REMINDER_HOURS"] = "-1"
	_, err = Load(env(m))
	require.ErrorContains(t, err, "STAGE_REMINDER_HOURS")
}

func TestLoadReadsEveryWorkloadSetting(t *testing.T) {
	m := required()
	for k, v := range map[string]string{
		"WORKLOAD_OIDC_JWKS_URL": "https://issuer.example.org/openid/v1/jwks", "WORKLOAD_OIDC_CA_FILE": "/oidc/ca.crt",
		"WORKLOAD_OIDC_BEARER_FILE": "/oidc/token", "WORKLOAD_AUDIENCE": "steward",
		"WORKLOAD_ALLOWED_SERVICEACCOUNTS": "steward/steward-gateway, steward/steward-identity",
		"WORKLOAD_TOKEN_FILE":              "/run/token",
	} {
		m[k] = v
	}
	c, err := Load(env(m))
	require.NoError(t, err)
	require.Equal(t, workloadauth.Config{
		Issuer: "https://issuer.example.org", JWKSURL: "https://issuer.example.org/openid/v1/jwks", CAFile: "/oidc/ca.crt",
		BearerFile: "/oidc/token", Audience: "steward", AllowedServiceAccounts: []string{"steward/steward-gateway", "steward/steward-identity"},
	}, c.WorkloadAuth)
	require.Equal(t, "/run/token", c.TokenFile)
}

func TestLoadFailsClosedWithoutWorkloadAuth(t *testing.T) {
	m := required()
	delete(m, "WORKLOAD_OIDC_ISSUER")
	_, err := Load(env(m))
	require.ErrorIs(t, err, workloadauth.ErrNotConfigured, "no issuer and no explicit off switch stops the boot")
}

func TestLoadTurnsWorkloadAuthOffOnlyWhenDisabled(t *testing.T) {
	m := required()
	delete(m, "WORKLOAD_OIDC_ISSUER")
	delete(m, "WORKLOAD_ALLOWED_SERVICEACCOUNTS")
	m["WORKLOAD_AUTH"] = "disabled"
	c, err := Load(env(m))
	require.NoError(t, err)
	require.False(t, c.WorkloadAuthEnabled)
	require.Empty(t, c.TokenFile, "switched off, workflow sends no token unless told where it is")

	// A peer can still enforce while this callee is switched off, so a set
	// token file is still sent.
	m["WORKLOAD_TOKEN_FILE"] = "/run/token"
	c, err = Load(env(m))
	require.NoError(t, err)
	require.Equal(t, "/run/token", c.TokenFile)
}

// A bad workload setting stops the boot instead of quietly falling back.
func TestLoadRejectsBadWorkloadSettings(t *testing.T) {
	for name, kv := range map[string][2]string{
		"auth mode typo":          {"WORKLOAD_AUTH", "off"},
		"disabled with an issuer": {"WORKLOAD_AUTH", "disabled"},
		"plain http issuer":       {"WORKLOAD_OIDC_ISSUER", "http://issuer.example.org"},
		"plain http jwks":         {"WORKLOAD_OIDC_JWKS_URL", "http://issuer.example.org/jwks"},
		"no allow-list":           {"WORKLOAD_ALLOWED_SERVICEACCOUNTS", ""},
		"bad allow-list entry":    {"WORKLOAD_ALLOWED_SERVICEACCOUNTS", "steward-gateway"},
	} {
		m := required()
		m[kv[0]] = kv[1]
		_, err := Load(env(m))
		require.Error(t, err, name)
		require.True(t, strings.Contains(err.Error(), kv[0]), "%s: the error names %s: %v", name, kv[0], err)
	}
}

func TestLoadTLSIsAllOrNothing(t *testing.T) {
	m := required()
	m["GRPC_TLS_CERT_FILE"] = "tls.crt"
	_, err := Load(env(m))
	require.ErrorContains(t, err, "set together")
}
