// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package outage

import (
	"context"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthCheckCaller is a function shape matching the standard gRPC health
// service's Check method minus the variadic call options (which we never set
// from the probe path). Using a function adapter keeps the outage package free
// of any direct *grpc.ClientConn dependency; cmd/server binds the closure from
// the shared identity connection, and tests can pass a fake.
type HealthCheckCaller func(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error)

// IdentityHealthCheck returns a HealthCheck that probes identity via the
// standard grpc.health.v1.Health/Check RPC (empty service = overall server
// health). Healthy means no error and SERVING; a transport error or any other
// status counts as a failure.
//
// Callers wire this from cmd/server; tests can pass a fake HealthCheckCaller.
func IdentityHealthCheck(call HealthCheckCaller) HealthCheck {
	return func(ctx context.Context) error {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, err := call(probeCtx, &healthpb.HealthCheckRequest{Service: ""})
		if err != nil {
			// Transport error (Unavailable / DeadlineExceeded / Canceled /
			// connection refused) — a real outage signal.
			return err
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			// Reachable but reporting NOT_SERVING / UNKNOWN — treat as down.
			return &notServingError{status: resp.GetStatus()}
		}
		return nil
	}
}

// notServingError describes a health response whose status is not SERVING.
type notServingError struct {
	status healthpb.HealthCheckResponse_ServingStatus
}

func (e *notServingError) Error() string {
	return "identity health check reported non-SERVING status: " + e.status.String()
}
