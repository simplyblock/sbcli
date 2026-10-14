package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// RPCError is a JSON-RPC error answer from the node.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// Caller sends one JSON-RPC request to a node.
type Caller interface {
	Call(ctx context.Context, method string, params any, out any) error
}

// HTTPCaller talks to the node's SPDK HTTP proxy the way simplyblock_core/rpc_client.py does:
// POST {"id","method","params"} to {scheme}://{mgmt_ip}:{rpc_port}/ with basic auth.
type HTTPCaller struct {
	URL      string
	Username string
	Password string
	Client   *http.Client
}

func (c *HTTPCaller) Call(ctx context.Context, method string, params any, out any) error {
	body := map[string]any{"id": 1, "method": method}
	if params != nil {
		body["params"] = params
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: http %d", method, resp.StatusCode)
	}
	var answer struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if answer.Error != nil {
		return answer.Error
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(answer.Result, out)
}

// TLSConfig mirrors simplyblock_core.settings: SB_TLS_CONNECT is disabled, anonymous or
// authenticated; the CA, certificate and key come from SB_TLS_CERTIFICATE_AUTHORITY,
// SB_TLS_CERTIFICATE and SB_TLS_KEY.
type TLSConfig struct {
	Mode, CA, Cert, Key string
}

// TLSConfigFromEnv reads the SB_TLS_* variables with the Python defaults.
func TLSConfigFromEnv() TLSConfig {
	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	return TLSConfig{
		Mode: strings.ToLower(get("SB_TLS_CONNECT", "disabled")),
		CA:   get("SB_TLS_CERTIFICATE_AUTHORITY", "/etc/simplyblock/tls/ca.crt"),
		Cert: get("SB_TLS_CERTIFICATE", "/etc/simplyblock/tls/tls.crt"),
		Key:  get("SB_TLS_KEY", "/etc/simplyblock/tls/tls.key"),
	}
}

// Scheme is the URL scheme for the mode.
func (t TLSConfig) Scheme() string {
	if t.Mode == "" || t.Mode == "disabled" {
		return "http"
	}
	return "https"
}

// HTTPClient builds the client for the mode. The overall timeout must exceed the long-poll
// timeout, so it is set per request through the context instead.
func (t TLSConfig) HTTPClient() (*http.Client, error) {
	tr := &http.Transport{MaxIdleConnsPerHost: 2, IdleConnTimeout: 90 * time.Second}
	if t.Scheme() == "https" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		pem, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, fmt.Errorf("tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls ca: no certificate in %s", t.CA)
		}
		cfg.RootCAs = pool
		if t.Mode == "authenticated" {
			cert, err := tls.LoadX509KeyPair(t.Cert, t.Key)
			if err != nil {
				return nil, fmt.Errorf("tls client certificate: %w", err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		tr.TLSClientConfig = cfg
	}
	return &http.Client{Transport: tr}, nil
}
