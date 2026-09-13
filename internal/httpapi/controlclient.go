package httpapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"

	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1/controlv1connect"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1/definitionvalidationv1connect"
)

// ControlClients are the private clients this service holds for
// anvilkit-agent-control.
//
// Each field is the narrow interface one surface calls, not the generated
// client itself, so a handler can reach only the methods its route declares.
// The generated ControlService client exposes Control's whole private surface;
// binding it here to a single method is what keeps that surface out of the
// public boundary, because no generic Control passthrough exists.
type ControlClients struct {
	// Validation forwards POST /v1/definitions/validations.
	Validation DefinitionValidator
	// Disclosure answers whether a caller may read one operation.
	Disclosure DisclosureAuthorizer
	// Commands forwards the public commands this service accepts.
	Commands OperationCommander
	// Preparations forwards the preparation answers and read (S2).
	Preparations PreparationCommander
}

// ControlClientConfig separates service TLS identity from the existing local
// validation grant. All credentials are deployment inputs loaded at startup.
type ControlClientConfig struct {
	Endpoint, CAFile, CertificateFile, KeyFile string
	ValidationToken                            string
	DialTimeout                                time.Duration
}

// NewControlClient builds Protobuf gRPC clients over HTTP/2. HTTPS requires
// mTLS; h2c is limited to numeric loopback addresses for controlled tests.
// Per-call deadlines come from the request context.
func NewControlClient(cfg ControlClientConfig) (ControlClients, error) {
	endpoint, dialTimeout := cfg.Endpoint, cfg.DialTimeout
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return ControlClients{}, fmt.Errorf("httpapi: the Control endpoint is not a URL: %w", err)
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return ControlClients{}, fmt.Errorf("httpapi: the Control endpoint must be a base URL without credentials, query or fragment")
	}
	if len(cfg.ValidationToken) < 32 || len(cfg.ValidationToken) > 256 || strings.ContainsAny(cfg.ValidationToken, " \t\r\n") {
		return ControlClients{}, fmt.Errorf("httpapi: Control requires a 32-256 character validation credential without whitespace")
	}

	var httpClient *http.Client
	switch parsed.Scheme {
	case "https":
		certificate, err := tls.LoadX509KeyPair(cfg.CertificateFile, cfg.KeyFile)
		if err != nil {
			return ControlClients{}, fmt.Errorf("httpapi: cannot load the Control client certificate and key")
		}
		ca, err := os.ReadFile(cfg.CAFile)
		roots := x509.NewCertPool()
		if err != nil || !roots.AppendCertsFromPEM(ca) {
			return ControlClients{}, fmt.Errorf("httpapi: cannot load the Control CA")
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ForceAttemptHTTP2 = true
		transport.DialContext = (&net.Dialer{Timeout: dialTimeout}).DialContext
		transport.TLSHandshakeTimeout = dialTimeout
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}}
		httpClient = &http.Client{Transport: transport}
	case "http":
		if ip := net.ParseIP(parsed.Hostname()); ip == nil || !ip.IsLoopback() || cfg.CAFile != "" || cfg.CertificateFile != "" || cfg.KeyFile != "" {
			return ControlClients{}, fmt.Errorf("httpapi: cleartext Control requires a numeric loopback endpoint and no TLS inputs")
		}
		dialer := &net.Dialer{Timeout: dialTimeout}
		httpClient = &http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, address)
			},
		}}
	default:
		return ControlClients{}, fmt.Errorf("httpapi: the Control endpoint scheme %q is not supported", parsed.Scheme)
	}

	// Private credentials must never follow redirects to a different authority.
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	validationCredential := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			request.Header().Set("Authorization", "Bearer "+cfg.ValidationToken)
			return next(ctx, request)
		}
	})
	control := controlv1connect.NewControlServiceClient(httpClient, endpoint, connect.WithGRPC())
	return ControlClients{
		Validation:   definitionvalidationv1connect.NewDefinitionValidationClient(httpClient, endpoint, connect.WithGRPC(), connect.WithInterceptors(validationCredential)),
		Disclosure:   control,
		Commands:     control,
		Preparations: control,
	}, nil
}
