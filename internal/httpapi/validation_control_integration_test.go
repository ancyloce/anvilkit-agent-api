//go:build controlintegration

package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/contracts"
)

// This test speaks public HTTP to the actual API handler and private mTLS/gRPC
// to the independently built Control executable. Neither service has a database,
// Temporal or external-business dependency in this validation-only proof.
func TestRealControlValidation(t *testing.T) {
	binary := os.Getenv("ANVILKIT_API_TEST_CONTROL_BINARY")
	if binary == "" {
		t.Fatal("ANVILKIT_API_TEST_CONTROL_BINARY is required; run the parent's api-validation proof")
	}
	dir := t.TempDir()
	ca, issue := validationCertificates(t, dir)
	serverCert, serverKey := issue("anvilkit-agent-control", true)
	apiCert, apiKey := issue("anvilkit-agent-api", false)
	otherCert, otherKey := issue("unmapped-service", false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	token := rand.Text() + rand.Text()
	logs, err := os.Create(filepath.Join(dir, "control.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	process := exec.Command(binary)
	// A deliberately minimal environment prevents ambient database or disclosure
	// configuration from activating another surface in the child process.
	process.Env = []string{
		"ANVILKIT_CONTROL_LISTEN_ADDR=" + address,
		"ANVILKIT_CONTROL_DEVELOPMENT_TOKEN=" + token,
		"ANVILKIT_CONTROL_TLS_CLIENT_CA=" + ca,
		"ANVILKIT_CONTROL_TLS_CERT=" + serverCert,
		"ANVILKIT_CONTROL_TLS_KEY=" + serverKey,
	}
	process.Stdout, process.Stderr = logs, logs
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() {
		_ = process.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Control did not stop cleanly: %v", err)
			}
		case <-time.After(5 * time.Second):
			_ = process.Process.Kill()
			<-done
			t.Error("Control shutdown exceeded its deadline")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Control did not open its listener")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cfg := ControlClientConfig{Endpoint: "https://" + address, DialTimeout: time.Second, CAFile: ca, CertificateFile: apiCert, KeyFile: apiKey, ValidationToken: token}
	clients, err := NewControlClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, clients.Validation)
	var logMu sync.Mutex
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logMu.Lock()
		defer logMu.Unlock()
		h.handler.ServeHTTP(w, r)
	}))
	defer api.Close()
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct {
			ID       string           `json:"id"`
			Request  json.RawMessage  `json:"request"`
			Response validationReport `json:"response"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(inputs["validation-positive-v1.fixtures.json"], &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures.Cases) != 2 {
		t.Fatal("expected the two retained positive fixtures")
	}
	call := func(t *testing.T, body []byte, publicToken string, want int) []byte {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, api.URL+routeDefinitionValidations, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+publicToken)
		// Untrusted service and actor headers confer no private authority.
		request.Header.Set("X-Actor-Id", "spoofed-actor")
		request.Header.Set("X-Tenant-Id", "spoofed-tenant")
		response, err := api.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err = io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("HTTP status=%d, want %d; response=%s", response.StatusCode, want, body)
		}
		if response.Header.Get("Location") != "" {
			t.Fatal("validation created an operation location")
		}
		return body
	}
	for _, fixture := range fixtures.Cases {
		t.Run(fixture.ID, func(t *testing.T) {
			var report validationReport
			if err := json.Unmarshal(call(t, fixture.Request, developerToken, 200), &report); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(report, fixture.Response) {
				t.Fatalf("unexpected retained fixture report: %+v", report)
			}
			// Equivalent numeric spelling must reach Control's canonicalizer.
			variant := bytes.Replace(fixture.Request, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": 1.0`), 1)
			if err := json.Unmarshal(call(t, variant, developerToken, 200), &report); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(report, fixture.Response) {
				t.Fatal("equivalent version changed the digest")
			}
		})
	}
	body := fixtures.Cases[0].Request
	// A semantic graph failure remains an HTTP 200 report, without a digest.
	invalid := bytes.Replace(body, []byte(`"entry": "plan"`), []byte(`"entry": "missing"`), 1)
	var report map[string]json.RawMessage
	if err := json.Unmarshal(call(t, invalid, developerToken, 200), &report); err != nil {
		t.Fatal(err)
	}
	if string(report["valid"]) != "false" || report["definitionDigest"] != nil || len(report["issues"]) <= 2 {
		t.Fatal("invalid definition report was not preserved")
	}
	call(t, body, "unmapped-public-credential", 401)
	call(t, body, readerToken, 403)
	call(t, bytes.Replace(body, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": 1,"unrecognizedAuthority": true`), 1), developerToken, 400)
	call(t, bytes.Replace(body, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": "1"`), 1), developerToken, 400)
	call(t, bytes.Replace(body, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": 1,"schemaVersion": 1`), 1), developerToken, 400)
	call(t, bytes.Replace(body, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": null`), 1), developerToken, 400)
	call(t, append(append([]byte{}, bytes.TrimSpace(body)[:len(bytes.TrimSpace(body))-1]...), []byte(`,"tenantId":"spoofed"}`)...), developerToken, 400)
	call(t, append(append([]byte{}, body...), bytes.Repeat([]byte(" "), 65537-len(body))...), developerToken, 400)
	// Same-CA certificates with another service name, a wrong method credential,
	// and missing client certificates cannot turn a public request into a report.
	for _, c := range []ControlClientConfig{
		{Endpoint: cfg.Endpoint, DialTimeout: time.Second, CAFile: ca, CertificateFile: otherCert, KeyFile: otherKey, ValidationToken: token},
		{Endpoint: cfg.Endpoint, DialTimeout: time.Second, CAFile: ca, CertificateFile: apiCert, KeyFile: apiKey, ValidationToken: rand.Text() + rand.Text()},
	} {
		client, err := NewControlClient(c)
		if err != nil {
			t.Fatal(err)
		}
		request := &validateDefinitionRequest{}
		if err := json.Unmarshal(body, request); err != nil {
			t.Fatal(err)
		}
		if result, fault := newHarness(t, client.Validation).server.forwardValidateDefinition(context.Background(), request); fault == nil || result != nil {
			t.Fatal("unmapped private caller received a report")
		}
	}
	rootBytes, _ := os.ReadFile(ca)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(rootBytes)
	transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}}
	defer transport.CloseIdleConnections()
	if response, err := (&http.Client{Transport: transport, Timeout: time.Second}).Get(cfg.Endpoint); err == nil {
		response.Body.Close()
		t.Fatal("Control accepted a client without a certificate")
	}
	logMu.Lock()
	defer logMu.Unlock()
	controlLogs, err := os.ReadFile(logs.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{token, developerToken, "spoofed-actor", "spoofed-tenant", `"definition":`, `"steps":`, `"operationId":`} {
		if bytes.Contains(controlLogs, []byte(forbidden)) || strings.Contains(h.logs.String(), forbidden) {
			t.Fatalf("validation logs disclosed forbidden data: %s", strings.ReplaceAll(forbidden, token, "[private credential]"))
		}
	}
}

func validationCertificates(t *testing.T, dir string) (string, func(string, bool) (string, string)) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "validation-test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	serial := int64(1)
	return caFile, func(name string, server bool) (string, string) {
		t.Helper()
		serial++
		pub, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		certificate := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			certificate.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			certificate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, certificate, ca, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		pkcs8, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		certFile, keyFile := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
		if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), 0600); err != nil {
			t.Fatal(err)
		}
		return certFile, keyFile
	}
}
