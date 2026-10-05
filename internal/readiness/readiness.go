// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package readiness registers workflow's dependencies with go-buildinfo's
// health checker. Postgres and RabbitMQ are required: without them workflow
// can't record a decision or audit it. Identity and core are optional: an
// identity outage already pauses every run through the outage reconciler, and
// a core outage fails only the calls that need core, so either degrades
// workflow instead of draining it.
package readiness

import (
	"context"
	"errors"
	"strings"

	"github.com/Bugs5382/go-buildinfo/health"
	postgres "github.com/Bugs5382/go-postgres"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Dependency names, as they appear in the report and the
// steward-depstate-<name> headers.
const (
	Postgres = "postgres"
	RabbitMQ = "rabbitmq"
	Identity = "identity"
	Core     = "core"
)

// Database is the Postgres the service runs on.
type Database interface {
	Ping(ctx context.Context) error
	ServerVersion(ctx context.Context) (string, error)
}

// Broker is the RabbitMQ connection; go-rabbitmq's Conn reports it.
type Broker interface{ Healthy() bool }

// Deps are the dependencies to report.
type Deps struct {
	Postgres Database
	Broker   Broker
	Identity func(ctx context.Context) error
	Core     func(ctx context.Context) error
}

var (
	errBrokerDown = errors.New("rabbitmq connection is down")
	errNotServing = errors.New("peer is not serving")
)

// New returns a checker with deps registered.
func New(d Deps, opts ...health.Option) (*health.Checker, error) {
	c := health.New(opts...)
	return c, c.Register(
		health.Dependency{Name: Postgres, Required: true, Check: d.Postgres.Ping, Version: d.Postgres.ServerVersion},
		health.Dependency{Name: RabbitMQ, Required: true, Check: func(context.Context) error {
			if !d.Broker.Healthy() {
				return errBrokerDown
			}
			return nil
		}},
		health.Dependency{Name: Identity, Check: d.Identity},
		health.Dependency{Name: Core, Check: d.Core},
	)
}

// GRPCPeer checks a peer service with the standard gRPC health check.
func GRPCPeer(conn grpc.ClientConnInterface) func(ctx context.Context) error {
	client := healthpb.NewHealthClient(conn)
	return func(ctx context.Context) error {
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return errNotServing
		}
		return nil
	}
}

// PostgresDB adapts go-postgres's DB.
func PostgresDB(db *postgres.DB) Database { return pgDB{db} }

type pgDB struct{ db *postgres.DB }

func (p pgDB) Ping(ctx context.Context) error { return p.db.Ping(ctx) }

// ServerVersion drops the build suffix ("16.4 (Debian 16.4-1)"), which the
// header would redact.
func (p pgDB) ServerVersion(ctx context.Context) (string, error) {
	var v string
	if err := p.db.Pool().QueryRow(ctx, "SHOW server_version").Scan(&v); err != nil {
		return "", err
	}
	if f := strings.Fields(v); len(f) > 0 {
		return f[0], nil
	}
	return v, nil
}
