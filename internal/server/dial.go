// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	gootel "github.com/Bugs5382/go-otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// DialOptions are the options for every outbound connection (core and
// identity). The go-grpc-actor client interceptors put the request's actor,
// and during act-as the real admin, on every call, so a peer records who
// really acted; a background call carries none. With a certificate the
// connection is mTLS, the same CA verifying the peer.
func DialOptions(certFile, keyFile, caFile string) ([]grpc.DialOption, error) {
	creds := insecure.NewCredentials()
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("dial: load certificate: %w", err)
		}
		pem, err := os.ReadFile(filepath.Clean(caFile))
		if err != nil {
			return nil, fmt.Errorf("dial: read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("dial: CA file holds no certificate")
		}
		creds = credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS13})
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(gootel.GRPCClientStatsHandler()),
		grpc.WithChainUnaryInterceptor(grpcactor.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(grpcactor.StreamClientInterceptor()),
	}, nil
}
