// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func required() map[string]string {
	return map[string]string{
		"DATABASE_DSN": "postgres://workflow:workflow@localhost:5432/workflow",
		"RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
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
	require.Empty(t, c.TrustedCallers)
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

func TestLoadTrustedCallersNeedTLS(t *testing.T) {
	m := required()
	m["WORKFLOW_TRUSTED_CALLERS"] = "spiffe://example.org/ns/steward/sa/gateway, spiffe://example.org/ns/steward/sa/identity"
	_, err := Load(env(m))
	require.ErrorContains(t, err, "GRPC_TLS")

	m["GRPC_TLS_CERT_FILE"], m["GRPC_TLS_KEY_FILE"], m["GRPC_TLS_CLIENT_CA_FILE"] = "tls.crt", "tls.key", "ca.crt"
	c, err := Load(env(m))
	require.NoError(t, err)
	require.Equal(t, []string{"spiffe://example.org/ns/steward/sa/gateway", "spiffe://example.org/ns/steward/sa/identity"}, c.TrustedCallers)
}

func TestLoadTLSIsAllOrNothing(t *testing.T) {
	m := required()
	m["GRPC_TLS_CERT_FILE"] = "tls.crt"
	_, err := Load(env(m))
	require.ErrorContains(t, err, "set together")
}
