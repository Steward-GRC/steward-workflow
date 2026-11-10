// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	workloadidentity "github.com/Bugs5382/go-workload-identity"
)

func TestWorkloadEnvDefaultsTheAudienceToSteward(t *testing.T) {
	env := map[string]string{workloadidentity.EnvIssuer: "https://issuer.example.org"}
	getenv := workloadEnv(func(k string) string { return env[k] })
	if got := getenv(workloadidentity.EnvAudience); got != WorkloadAudience {
		t.Fatalf("unset audience: got %q, want %q", got, WorkloadAudience)
	}
	env[workloadidentity.EnvAudience] = "other"
	if got := getenv(workloadidentity.EnvAudience); got != "other" {
		t.Fatalf("set audience: got %q, want other", got)
	}
	if got := getenv(workloadidentity.EnvIssuer); got != "https://issuer.example.org" {
		t.Fatalf("other variables pass through: got %q", got)
	}
}

func TestStewardWorkloadMapsTheServiceAccountPrefix(t *testing.T) {
	c := StewardWorkload(workloadidentity.Config{Issuer: "https://issuer.example.org"})
	if c.Audience != "steward" || c.ServiceAccountPrefix != "steward-" {
		t.Fatalf("got audience %q prefix %q, want steward and steward-", c.Audience, c.ServiceAccountPrefix)
	}
	c = StewardWorkload(workloadidentity.Config{Audience: "other", ServiceAccountPrefix: "app-"})
	if c.Audience != "other" || c.ServiceAccountPrefix != "steward-" {
		t.Fatalf("got audience %q prefix %q: a set audience stays, the prefix is always steward-", c.Audience, c.ServiceAccountPrefix)
	}
}

func TestServerWorkloadConfigCarriesStewardValues(t *testing.T) {
	env := map[string]string{
		workloadidentity.EnvIssuer:                 "https://issuer.example.org",
		workloadidentity.EnvAllowedServiceAccounts: "steward/steward-gateway",
	}
	c, enabled, err := serverWorkloadConfig(func(k string) string { return env[k] })
	if err != nil || !enabled {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}
	if c.Audience != WorkloadAudience || c.ServiceAccountPrefix != WorkloadServiceAccountPrefix {
		t.Fatalf("got audience %q prefix %q", c.Audience, c.ServiceAccountPrefix)
	}

	env = map[string]string{workloadidentity.EnvAuthMode: workloadidentity.AuthDisabled}
	if _, enabled, err = serverWorkloadConfig(func(k string) string { return env[k] }); err != nil || enabled {
		t.Fatalf("disabled: enabled=%v err=%v", enabled, err)
	}
}
