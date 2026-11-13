// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package config reads the workflow service's settings from the environment.
package config

import (
	"errors"
	"fmt"
	"strconv"

	workloadidentity "github.com/Bugs5382/go-workload-identity"
)

// TLS is the service certificate and the CA its peers' certificates chain to.
// It serves mTLS and dials core and identity with it. Empty serves and dials
// plain gRPC.
type TLS struct {
	CertFile     string
	KeyFile      string
	ClientCAFile string
}

// Enabled reports whether TLS is configured.
func (t TLS) Enabled() bool { return t.CertFile != "" }

// Config is every setting the service runs with.
type Config struct {
	DatabaseDSN string
	// MigrateDSN is a direct connection for migrations; defaults to
	// DatabaseDSN.
	MigrateDSN string
	// SagaDatabaseDSN holds the embedded saga engine's tables; defaults to
	// DatabaseDSN.
	SagaDatabaseDSN string
	MigrationsDir   string
	RabbitURL       string
	GRPCPort        string
	// ProbePort serves /livez and /readyz over plain HTTP.
	ProbePort string
	// OTLPEndpoint is the OpenTelemetry collector; empty turns tracing off.
	OTLPEndpoint string
	IdentityAddr string
	CoreAddr     string
	// StageSLAHours is a stage's SLA when it sets none.
	StageSLAHours int
	// StageReminderHours is when its reminder falls.
	StageReminderHours int
	// StageNoticeSweepMinutes is how often the sweeper looks for overdue
	// stages to remind and escalate (steward-workflow#76).
	StageNoticeSweepMinutes int
	TLS                     TLS
	// WorkloadAuth verifies the callers' workload tokens. It is set only
	// while WorkloadAuthEnabled, which is false only with
	// WORKLOAD_AUTH=disabled.
	WorkloadAuth        workloadidentity.Config
	WorkloadAuthEnabled bool
	// TokenFile is workflow's own projected token, sent to core and identity
	// on every call. It defaults to WorkloadTokenFile while
	// authentication is on; switched off, it is sent only when
	// WORKLOAD_TOKEN_FILE is set, because a peer may still enforce.
	TokenFile string
}

// Load reads the settings through getenv (os.Getenv in production).
func Load(getenv func(string) string) (Config, error) {
	or := func(k, d string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return d
	}
	c := Config{
		DatabaseDSN:   getenv("DATABASE_DSN"),
		MigrationsDir: or("MIGRATIONS_DIR", "migrations"),
		RabbitURL:     getenv("RABBITMQ_URL"),
		GRPCPort:      or("GRPC_PORT", "9092"),
		ProbePort:     or("PROBE_PORT", "8080"),
		OTLPEndpoint:  getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		IdentityAddr:  or("IDENTITY_GRPC_ADDR", "identity:9090"),
		CoreAddr:      or("CORE_GRPC_ADDR", "core:9090"),
		TLS:           TLS{CertFile: getenv("GRPC_TLS_CERT_FILE"), KeyFile: getenv("GRPC_TLS_KEY_FILE"), ClientCAFile: getenv("GRPC_TLS_CLIENT_CA_FILE")},
	}
	c.MigrateDSN = or("MIGRATE_DSN", c.DatabaseDSN)
	c.SagaDatabaseDSN = or("SAGA_DATABASE_DSN", c.DatabaseDSN)

	var errs []error
	var err error
	if c.WorkloadAuth, c.WorkloadAuthEnabled, err = serverWorkloadConfig(getenv); err != nil {
		errs = append(errs, err)
	}
	c.TokenFile = getenv(workloadidentity.EnvTokenFile)
	if c.TokenFile == "" && c.WorkloadAuthEnabled {
		c.TokenFile = WorkloadTokenFile
	}
	if c.DatabaseDSN == "" {
		errs = append(errs, errors.New("DATABASE_DSN is required"))
	}
	if c.RabbitURL == "" {
		errs = append(errs, errors.New("RABBITMQ_URL is required"))
	}
	hours := func(k string, d int) int {
		n, err := strconv.Atoi(or(k, strconv.Itoa(d)))
		if err != nil || n < 0 {
			errs = append(errs, fmt.Errorf("%s must be a whole number of hours, 0 or more", k))
		}
		return n
	}
	c.StageSLAHours = hours("STAGE_SLA_HOURS", 72)
	c.StageReminderHours = hours("STAGE_REMINDER_HOURS", 48)
	c.StageNoticeSweepMinutes = 15
	if v := getenv("STAGE_NOTICE_SWEEP_MINUTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			errs = append(errs, errors.New("STAGE_NOTICE_SWEEP_MINUTES must be a whole number of minutes, 1 or more"))
		} else {
			c.StageNoticeSweepMinutes = n
		}
	}
	tlsSet := c.TLS.CertFile != "" || c.TLS.KeyFile != "" || c.TLS.ClientCAFile != ""
	if tlsSet && (c.TLS.CertFile == "" || c.TLS.KeyFile == "" || c.TLS.ClientCAFile == "") {
		errs = append(errs, errors.New("GRPC_TLS_CERT_FILE, GRPC_TLS_KEY_FILE and GRPC_TLS_CLIENT_CA_FILE are set together"))
	}
	return c, errors.Join(errs...)
}
