// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bugs5382/go-buildinfo/health"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/readiness"
	"github.com/Steward-GRC/steward-workflow/internal/workloadauth"
)

const testNS = "steward"

// localIssuer is an OIDC issuer on a local TLS test server: discovery and a
// JWKS with one P-256 key generated in the test.
type localIssuer struct {
	url, caFile string
	key         *ecdsa.PrivateKey
	// jwksStatus, when set, is the status the JWKS endpoint answers instead
	// of the key set.
	jwksStatus atomic.Int32
}

func newLocalIssuer(t *testing.T) *localIssuer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	iss := &localIssuer{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": iss.url, "jwks_uri": iss.url + "/openid/v1/jwks"})
	})
	mux.HandleFunc("/openid/v1/jwks", func(w http.ResponseWriter, _ *http.Request) {
		if code := iss.jwksStatus.Load(); code != 0 {
			http.Error(w, http.StatusText(int(code)), int(code))
			return
		}
		pub, err := key.PublicKey.ECDH()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		raw := pub.Bytes()
		b64 := base64.RawURLEncoding.EncodeToString
		w.Header().Set("Content-Type", "application/jwk-set+json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "EC", "kid": "k1", "use": "sig", "crv": "P-256", "x": b64(raw[1:33]), "y": b64(raw[33:]),
		}}})
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	iss.url = srv.URL
	iss.caFile = filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(iss.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	return iss
}

// tokenAt signs a token for sa that is valid from issued for an hour.
func (i *localIssuer) tokenAt(t *testing.T, sa, audience string, issued time.Time) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": i.url, "aud": []string{audience}, "sub": "system:serviceaccount:" + testNS + ":" + sa,
		"iat": issued.Unix(), "nbf": issued.Unix(), "exp": issued.Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(i.key)
	require.NoError(t, err)
	return s
}

func (i *localIssuer) token(t *testing.T, sa, audience string) string {
	t.Helper()
	return i.tokenAt(t, sa, audience, time.Now())
}

func (i *localIssuer) verifier(t *testing.T, allowed ...string) *workloadauth.Verifier {
	t.Helper()
	v, err := workloadauth.NewVerifier(workloadauth.Config{
		Issuer: i.url, CAFile: i.caFile, Audience: "steward", AllowedServiceAccounts: allowed,
	}, log.Nop())
	require.NoError(t, err)
	return v
}

var getStatus = workflowv1.WorkflowService_GetStatus_FullMethodName

// authServe serves the probe with workload auth on: steward-gateway,
// steward-identity and steward-reporting hold valid identities, but the
// policy lists only the gateway (on behalf) and identity (as itself).
func authServe(t *testing.T) (*localIssuer, *grpc.ClientConn, chan bool, func()) {
	t.Helper()
	iss := newLocalIssuer(t)
	v := iss.verifier(t, testNS+"/steward-gateway", testNS+"/steward-identity", testNS+"/steward-reporting")
	require.NoError(t, v.Refresh(context.Background()))
	conn, saw, stop := serve(t, Options{Auth: &Auth{
		Verifier: v,
		Policy:   workloadauth.Policy{getStatus: {"gateway": workloadauth.OnBehalf, "identity": workloadauth.Self}},
	}})
	return iss, conn, saw, stop
}

func bearerCtx(tok string) context.Context {
	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "erin", Impersonator: "alice"})
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func callStatus(conn *grpc.ClientConn, ctx context.Context) codes.Code {
	_, err := workflowv1.NewWorkflowServiceClient(conn).GetStatus(ctx, &workflowv1.GetStatusRequest{})
	return status.Code(err)
}

func requireHandlerDidNotRun(t *testing.T, saw chan bool) {
	t.Helper()
	select {
	case <-saw:
		t.Fatal("the handler ran without a verified, listed caller")
	default:
	}
}

func TestWorkloadAuthLetsTheGatewayForwardTheActor(t *testing.T) {
	iss, conn, saw, stop := authServe(t)
	defer stop()
	require.Equal(t, codes.OK, callStatus(conn, bearerCtx(iss.token(t, "steward-gateway", "steward"))))
	require.True(t, <-saw, "the gateway's forwarded actor is believed")
}

func TestWorkloadAuthLetsASelfCallerActOnlyAsItself(t *testing.T) {
	iss, conn, saw, stop := authServe(t)
	defer stop()
	require.Equal(t, codes.OK, callStatus(conn, bearerCtx(iss.token(t, "steward-identity", "steward"))))
	require.False(t, <-saw, "a Self caller's forwarded actor is dropped")
}

func TestWorkloadAuthRefusesAnUnauthenticatedCall(t *testing.T) {
	_, conn, saw, stop := authServe(t)
	defer stop()
	require.Equal(t, codes.Unauthenticated, callStatus(conn, context.Background()), "no token")
	require.Equal(t, codes.Unauthenticated, callStatus(conn, bearerCtx("not-a-jwt")), "a malformed token")
	requireHandlerDidNotRun(t, saw)
}

func TestWorkloadAuthRefusesAWrongAudienceIssuerOrExpiredToken(t *testing.T) {
	iss, conn, saw, stop := authServe(t)
	defer stop()
	require.Equal(t, codes.Unauthenticated, callStatus(conn, bearerCtx(iss.token(t, "steward-gateway", "other"))), "another audience")
	other := newLocalIssuer(t)
	require.Equal(t, codes.Unauthenticated, callStatus(conn, bearerCtx(other.token(t, "steward-gateway", "steward"))), "another issuer")
	expired := iss.tokenAt(t, "steward-gateway", "steward", time.Now().Add(-2*time.Hour))
	require.Equal(t, codes.Unauthenticated, callStatus(conn, bearerCtx(expired)), "an expired token")
	requireHandlerDidNotRun(t, saw)
}

func TestWorkloadAuthRefusesACallerNotOnTheAllowList(t *testing.T) {
	iss, conn, saw, stop := authServe(t)
	defer stop()
	require.Equal(t, codes.PermissionDenied, callStatus(conn, bearerCtx(iss.token(t, "steward-reporting", "steward"))),
		"a verified identity the method doesn't list")
	require.Equal(t, codes.Unauthenticated, callStatus(conn, bearerCtx(iss.token(t, "steward-ai", "steward"))),
		"a valid token from a service account outside WORKLOAD_ALLOWED_SERVICEACCOUNTS")
	requireHandlerDidNotRun(t, saw)
}

func TestWorkloadAuthLeavesHealthAndReflectionOpen(t *testing.T) {
	_, conn, _, stop := authServe(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hc, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, hc.GetStatus())
	rs, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	require.NoError(t, err)
	require.NoError(t, rs.Send(&reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{}}))
	res, err := rs.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, res.GetListServicesResponse().GetService())
}

func TestWorkloadAuthDisabledLetsCallsThroughAndTrustsNoActor(t *testing.T) {
	conn, saw, stop := serve(t, Options{})
	defer stop()
	require.Equal(t, codes.OK, callStatus(conn, context.Background()), "WORKLOAD_AUTH=disabled serves a call with no token")
	require.False(t, <-saw, "and still believes no forwarded actor")
}

func TestTrustOnBehalfNeedsAnOnBehalfGrant(t *testing.T) {
	grant := func(a workloadauth.Access) context.Context {
		return workloadauth.ContextWithGrant(context.Background(), workloadauth.Grant{Caller: workloadauth.Caller{Name: "x"}, Access: a})
	}
	require.True(t, TrustOnBehalf(grant(workloadauth.OnBehalf), "/m"))
	require.False(t, TrustOnBehalf(grant(workloadauth.Self), "/m"))
	require.False(t, TrustOnBehalf(context.Background(), "/m"), "no verified caller, no trust")
}

type upDB struct{}

func (upDB) Ping(context.Context) error                    { return nil }
func (upDB) ServerVersion(context.Context) (string, error) { return "16.4", nil }

type upBroker struct{}

func (upBroker) Healthy() bool { return true }

func upPeer(context.Context) error { return nil }

// The issuer refusing the JWKS fetch (as an API server does for a bearer with
// the wrong audience) must never leave the verifier inert: a valid-looking
// token is refused as Unavailable, and readiness drains the pod on both
// probes.
func TestWorkloadAuthFailsClosedWhileTheJWKSIsRefused(t *testing.T) {
	iss := newLocalIssuer(t)
	iss.jwksStatus.Store(http.StatusUnauthorized)
	v := iss.verifier(t, testNS+"/steward-gateway")
	require.Error(t, v.Refresh(context.Background()), "a 401 from the JWKS is a failed refresh")

	checker, err := readiness.New(readiness.Deps{Postgres: upDB{}, Broker: upBroker{}, Identity: upPeer, Core: upPeer,
		JWKS: readiness.RecheckEvery(v.Refresh, time.Minute, time.Now)}, health.WithTTL(time.Millisecond))
	require.NoError(t, err)
	conn, saw, stop := serve(t, Options{Checker: checker, CheckInterval: 20 * time.Millisecond, Auth: &Auth{
		Verifier: v,
		Policy:   workloadauth.Policy{getStatus: {"gateway": workloadauth.OnBehalf}},
	}})
	defer stop()

	require.Equal(t, codes.Unavailable, callStatus(conn, bearerCtx(iss.token(t, "steward-gateway", "steward"))))
	requireHandlerDidNotRun(t, saw)

	hc := healthpb.NewHealthClient(conn)
	for _, svc := range []string{"", ReadinessService} {
		r, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, r.GetStatus(), "service %q", svc)
	}
	rep := checker.Report(context.Background())
	require.False(t, rep.Ready)
	var jwks *health.DependencyReport
	for i := range rep.Dependencies {
		if rep.Dependencies[i].Name == readiness.JWKS {
			jwks = &rep.Dependencies[i]
		}
	}
	require.NotNil(t, jwks, "the key set is listed in the readiness report")
	require.True(t, jwks.Required)
	require.Equal(t, health.StateDown, jwks.State)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeProbes(ctx, lis, checker) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	res, err := http.Get("http://" + lis.Addr().String() + "/readyz")
	require.NoError(t, err)
	_ = res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
}
