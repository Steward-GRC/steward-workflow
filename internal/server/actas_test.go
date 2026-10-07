// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/workloadauth"
)

// The act-as round trip over mTLS with workload tokens: the gateway forwards
// the target and the real admin; workflow believes them only from a verified
// caller with on-behalf access, and calls on to core with its own token.

type pki struct {
	dir    string
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caFile string
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Example test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	p := &pki{dir: t.TempDir(), ca: ca, caKey: key}
	p.caFile = p.write(t, "ca.crt", "CERTIFICATE", der)
	return p
}

func (p *pki) write(t *testing.T, name, kind string, der []byte) string {
	t.Helper()
	path := filepath.Join(p.dir, name)
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600))
	return path
}

// leaf issues a certificate for localhost with the SPIFFE ID as its URI SAN.
func (p *pki) leaf(t *testing.T, name, spiffe string, serial int64) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	u, err := url.Parse(spiffe)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{u},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	require.NoError(t, err)
	kder, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return p.write(t, name+".crt", "CERTIFICATE", der), p.write(t, name+".key", "EC PRIVATE KEY", kder)
}

// actorProbe records the actor each call arrives with and, when next is
// set, calls next with the context it was given.
type actorProbe struct {
	workflowv1.UnimplementedWorkflowServiceServer
	seen chan grpcactor.Actor
	next workflowv1.WorkflowServiceClient
}

func (p actorProbe) GetStatus(ctx context.Context, req *workflowv1.GetStatusRequest) (*workflowv1.GetStatusResponse, error) {
	a, _ := grpcactor.FromContext(ctx)
	p.seen <- a
	if p.next != nil {
		return p.next.GetStatus(ctx, req)
	}
	return &workflowv1.GetStatusResponse{}, nil
}

func serveTLS(t *testing.T, p *pki, name string, auth *Auth, probe actorProbe) string {
	t.Helper()
	cert, key := p.leaf(t, name, "spiffe://example.org/ns/steward/sa/"+name, time.Now().UnixNano())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, lis, log.Nop(), Options{CertFile: cert, KeyFile: key, ClientCAFile: p.caFile, Auth: auth},
			func(s *grpc.Server) { workflowv1.RegisterWorkflowServiceServer(s, probe) })
	}()
	t.Cleanup(func() { cancel(); <-done })
	return lis.Addr().String()
}

// tokenFile writes a caller token where DialOptions reads it.
func tokenFile(t *testing.T, tok string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(tok+"\n"), 0o600))
	return path
}

func dialAs(t *testing.T, p *pki, addr, tokFile string) workflowv1.WorkflowServiceClient {
	t.Helper()
	cert, key := p.leaf(t, "caller", "spiffe://example.org/ns/steward/sa/caller", time.Now().UnixNano())
	opts, err := DialOptions(cert, key, p.caFile, tokFile)
	require.NoError(t, err)
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return workflowv1.NewWorkflowServiceClient(conn)
}

func authFor(t *testing.T, iss *localIssuer, policy workloadauth.Policy) *Auth {
	t.Helper()
	v := iss.verifier(t, testNS+"/steward-gateway", testNS+"/steward-workflow", testNS+"/steward-reporting")
	require.NoError(t, v.Refresh(context.Background()))
	return &Auth{Verifier: v, Policy: policy}
}

func TestActAs_GatewayActorReachesWorkflowAndWorkflowCallsCoreAsItself(t *testing.T) {
	p, iss := newPKI(t), newLocalIssuer(t)
	// core lists workflow as Self: the call is accepted on workflow's token,
	// and the actor it forwards is not believed.
	coreSeen := make(chan grpcactor.Actor, 1)
	coreAddr := serveTLS(t, p, "core", authFor(t, iss, workloadauth.Policy{getStatus: {"workflow": workloadauth.Self}}), actorProbe{seen: coreSeen})

	workflowSeen := make(chan grpcactor.Actor, 1)
	toCore := dialAs(t, p, coreAddr, tokenFile(t, iss.token(t, "steward-workflow", "steward")))
	workflowAddr := serveTLS(t, p, "workflow", authFor(t, iss, workloadauth.Policy{getStatus: {"gateway": workloadauth.OnBehalf}}),
		actorProbe{seen: workflowSeen, next: toCore})

	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"})
	_, err := dialAs(t, p, workflowAddr, tokenFile(t, iss.token(t, "steward-gateway", "steward"))).GetStatus(ctx, &workflowv1.GetStatusRequest{})
	require.NoError(t, err)

	require.Equal(t, grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"}, <-workflowSeen, "workflow sees the target and the admin")
	require.Equal(t, grpcactor.Actor{}, <-coreSeen, "core takes workflow's call as workflow's own")
}

func TestActAs_UnlistedCallerIsRefused(t *testing.T) {
	p, iss := newPKI(t), newLocalIssuer(t)
	seen := make(chan grpcactor.Actor, 1)
	addr := serveTLS(t, p, "workflow", authFor(t, iss, workloadauth.Policy{getStatus: {"gateway": workloadauth.OnBehalf}}), actorProbe{seen: seen})

	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"})
	_, err := dialAs(t, p, addr, tokenFile(t, iss.token(t, "steward-reporting", "steward"))).GetStatus(ctx, &workflowv1.GetStatusRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, seen)
}

func TestActAs_SelfOnlyCallerActorIsDropped(t *testing.T) {
	p, iss := newPKI(t), newLocalIssuer(t)
	seen := make(chan grpcactor.Actor, 1)
	addr := serveTLS(t, p, "workflow", authFor(t, iss, workloadauth.Policy{getStatus: {"reporting": workloadauth.Self}}), actorProbe{seen: seen})

	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"})
	_, err := dialAs(t, p, addr, tokenFile(t, iss.token(t, "steward-reporting", "steward"))).GetStatus(ctx, &workflowv1.GetStatusRequest{})
	require.NoError(t, err, "a self-only caller is let in as itself")
	require.Equal(t, grpcactor.Actor{}, <-seen, "the actor it forwarded is not believed")
}

func TestActAs_SystemCallForwardsNothing(t *testing.T) {
	p, iss := newPKI(t), newLocalIssuer(t)
	seen := make(chan grpcactor.Actor, 1)
	addr := serveTLS(t, p, "core", authFor(t, iss, workloadauth.Policy{getStatus: {"workflow": workloadauth.OnBehalf}}), actorProbe{seen: seen})

	_, err := dialAs(t, p, addr, tokenFile(t, iss.token(t, "steward-workflow", "steward"))).GetStatus(context.Background(), &workflowv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, grpcactor.Actor{}, <-seen, "a background call carries no actor")
}

func TestDialOptionsSendTheTokenAndReadItOnEveryCall(t *testing.T) {
	iss := newLocalIssuer(t)
	seen := make(chan grpcactor.Actor, 2)
	addr := servePlain(t, authFor(t, iss, workloadauth.Policy{getStatus: {"workflow": workloadauth.Self}}), actorProbe{seen: seen})

	path := tokenFile(t, iss.token(t, "steward-workflow", "steward"))
	opts, err := DialOptions("", "", "", path)
	require.NoError(t, err)
	c, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	client := workflowv1.NewWorkflowServiceClient(c)
	_, err = client.GetStatus(context.Background(), &workflowv1.GetStatusRequest{})
	require.NoError(t, err, "the callee verified workflow's token")
	<-seen

	// The kubelet rotates the token in place; the next call reads the new one.
	require.NoError(t, os.WriteFile(path, []byte(iss.token(t, "steward-reporting", "steward")), 0o600))
	_, err = client.GetStatus(context.Background(), &workflowv1.GetStatusRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the rotated file is what went out")
}

func TestDialOptionsWithoutATokenFileSendNoToken(t *testing.T) {
	iss := newLocalIssuer(t)
	addr := servePlain(t, authFor(t, iss, workloadauth.Policy{getStatus: {"workflow": workloadauth.Self}}), actorProbe{seen: make(chan grpcactor.Actor, 1)})
	opts, err := DialOptions("", "", "", "")
	require.NoError(t, err)
	c, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	_, err = workflowv1.NewWorkflowServiceClient(c).GetStatus(context.Background(), &workflowv1.GetStatusRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestDialOptionsFailOnAnUnreadableTokenFile(t *testing.T) {
	_, err := DialOptions("", "", "", filepath.Join(t.TempDir(), "missing"))
	require.ErrorContains(t, err, "WORKLOAD_TOKEN_FILE", "a missing mount stops the boot instead of every call")
}

func servePlain(t *testing.T, auth *Auth, probe actorProbe) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, lis, log.Nop(), Options{Auth: auth}, func(s *grpc.Server) { workflowv1.RegisterWorkflowServiceServer(s, probe) })
	}()
	t.Cleanup(func() { cancel(); <-done })
	return lis.Addr().String()
}
