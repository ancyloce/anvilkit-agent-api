// Package identity is the workload identity of the outbound internal gRPC
// clients (P0.1, security.md SEC-01): rotating TLS 1.3 client credentials
// loaded from mounted certificate files, verifying each server against the
// current CA bundle and its reviewed server name. A workload certificate
// identifies a service; it never establishes a user, tenant, role or
// approver. The API serves no internal gRPC, so it holds no server
// credentials or caller authorization; the tests' peer servers use the
// fixture credentials of identitytest.
package identity

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
)

// clientCreds dials with the Reloader's current certificate and CA bundle
// and verifies the server through standard hostname verification against
// ServerName (a DNS SAN); no session cache, so nothing resumes under
// retired trust.
type clientCreds struct {
	reloader   *Reloader
	serverName string
}

// NewClientCredentials returns rotating client credentials; serverName is
// the DNS SAN the server certificate must carry.
func NewClientCredentials(r *Reloader, serverName string) (credentials.TransportCredentials, error) {
	if serverName == "" {
		return nil, errors.New("identity: server_name is required for hostname verification")
	}
	return &clientCreds{reloader: r, serverName: serverName}, nil
}

func (c *clientCreds) config() *tls.Config {
	m := c.reloader.Current()
	return &tls.Config{
		Certificates: []tls.Certificate{m.Certificate},
		RootCAs:      m.Pool,
		ServerName:   c.serverName,
		MinVersion:   tls.VersionTLS13,
	}
}

func (c *clientCreds) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return credentials.NewTLS(c.config()).ClientHandshake(ctx, authority, raw)
}

func (c *clientCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("identity: client credentials cannot serve")
}

func (c *clientCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3", ServerName: c.serverName}
}

func (c *clientCreds) Clone() credentials.TransportCredentials {
	return &clientCreds{reloader: c.reloader, serverName: c.serverName}
}

// OverrideServerName keeps the configured name: the reviewed server_name is
// the verification target, never the dialed authority.
func (c *clientCreds) OverrideServerName(string) error { return nil }
