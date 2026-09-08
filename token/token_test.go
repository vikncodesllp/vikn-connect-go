package token

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type keyPair struct {
	kid  string
	priv *rsa.PrivateKey
}

func newKey(t *testing.T, kid string) keyPair {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return keyPair{kid: kid, priv: priv}
}

func jwksServer(t *testing.T, hits *int32, keys ...keyPair) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(hits, 1)
		var set struct {
			Keys []map[string]string `json:"keys"`
		}
		for _, k := range keys {
			set.Keys = append(set.Keys, map[string]string{"kty": "RSA", "kid": k.kid, "alg": "RS256",
				"n": base64.RawURLEncoding.EncodeToString(k.priv.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.priv.E)).Bytes())})
		}
		_ = json.NewEncoder(w).Encode(set)
	}))
}

func sign(t *testing.T, k keyPair, claims Claims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = k.kid
	signed, err := tok.SignedString(k.priv)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func integrationClaims(org uuid.UUID, audience, scope string) Claims {
	return Claims{ClientID: "desk-connect", OrganizationID: org, Scope: scope, TokenUse: TokenUseIntegration,
		RegisteredClaims: jwt.RegisteredClaims{Subject: uuid.NewString(), Audience: jwt.ClaimStrings{audience}, Issuer: Issuer,
			IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}}
}

func TestVerifierAcceptsAValidIntegrationToken(t *testing.T) {
	var hits int32
	key := newKey(t, "vikn-auth-1")
	srv := jwksServer(t, &hits, key)
	defer srv.Close()
	org := uuid.New()
	v := NewVerifier(srv.URL)
	raw := sign(t, key, integrationClaims(org, "vikn-projects", "project.issues.read project.metadata.read"))

	claims, err := v.Verify(raw, "vikn-projects", "project.issues.read")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.OrganizationID != org || !claims.HasScope("project.metadata.read") {
		t.Fatalf("claims = %+v", claims)
	}
	if _, err := v.Verify(raw, "", "project.issues.read"); err != nil {
		t.Fatalf("any-audience verify: %v", err)
	}
	if _, err := v.Verify(raw, "vikn-desk"); err == nil {
		t.Fatal("wrong audience must fail")
	}
	if _, err := v.Verify(raw, "vikn-projects", "project.issues.write"); err == nil {
		t.Fatal("missing scope must fail")
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("JWKS fetched %d times, want once (keys are cached)", got)
	}
}

func TestVerifierRejectsForeignAndUserTokens(t *testing.T) {
	var hits int32
	key := newKey(t, "vikn-auth-1")
	stranger := newKey(t, "vikn-auth-1")
	srv := jwksServer(t, &hits, key)
	defer srv.Close()
	v := NewVerifier(srv.URL)
	org := uuid.New()

	if _, err := v.Verify(sign(t, stranger, integrationClaims(org, "vikn-projects", "x")), "vikn-projects"); err == nil {
		t.Fatal("a token signed by another key must fail")
	}
	user := integrationClaims(org, "vikn-projects", "x")
	user.TokenUse = ""
	if _, err := v.Verify(sign(t, key, user), "vikn-projects"); err == nil {
		t.Fatal("a non-integration token must fail")
	}
	expired := integrationClaims(org, "vikn-projects", "x")
	expired.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
	if _, err := v.Verify(sign(t, key, expired), "vikn-projects"); err == nil {
		t.Fatal("an expired token must fail")
	}
	if BearerFromHeader("Bearer  abc ") != "abc" || BearerFromHeader("Basic abc") != "" {
		t.Fatal("BearerFromHeader")
	}
}

func TestClientMintsResolvesAndCalls(t *testing.T) {
	org := uuid.New()
	var tokenCalls, resolveCalls, apiCalls int32
	var seenScope, seenAudience, seenBasic string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&apiCalls, 1)
		if r.Header.Get("Authorization") != "Bearer tok-1" || r.URL.Path != "/v1/issues/42" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"title":"Printer on fire"}}`))
	}))
	defer app.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			atomic.AddInt32(&tokenCalls, 1)
			user, pass, _ := r.BasicAuth()
			seenBasic = user + ":" + pass
			_ = r.ParseForm()
			seenScope, seenAudience = r.Form.Get("scope"), r.Form.Get("audience")
			if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("organization_id") != org.String() {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"access_token":"tok-1","token_type":"Bearer","expires_in":600}`))
		case "/api/connect/resolve":
			atomic.AddInt32(&resolveCalls, 1)
			if r.Header.Get("Authorization") != "Bearer tok-1" || r.URL.Query().Get("app") != "vikn-projects" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"app_slug":"vikn-projects","api_base_url":"` + app.URL + `/","api_version":"v1","region":"in-south","scopes":["project.issues.read"]}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer auth.Close()

	c := NewClient(Credentials{AuthURL: auth.URL, ClientID: "desk", ClientSecret: "s3cret"})
	var out struct {
		Data struct {
			Title string `json:"title"`
		} `json:"data"`
	}
	for i := 0; i < 3; i++ {
		if err := c.GetJSON(context.Background(), org, "vikn-projects", "/v1/issues/42", &out, "project.issues.read"); err != nil {
			t.Fatalf("GetJSON #%d: %v", i, err)
		}
	}
	if out.Data.Title != "Printer on fire" {
		t.Fatalf("out = %+v", out)
	}
	if tokenCalls != 1 || resolveCalls != 1 || apiCalls != 3 {
		t.Fatalf("token=%d resolve=%d api=%d, want 1/1/3 (caching)", tokenCalls, resolveCalls, apiCalls)
	}
	if seenScope != "project.issues.read" || seenAudience != "vikn-projects" || seenBasic != "desk:s3cret" {
		t.Fatalf("token request: scope=%q audience=%q basic=%q", seenScope, seenAudience, seenBasic)
	}

	// The cached token is dropped a minute before expiry.
	c.now = func() time.Time { return time.Now().Add(9*time.Minute + 30*time.Second) }
	if _, err := c.Token(context.Background(), org, "vikn-projects", "project.issues.read"); err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 2 {
		t.Fatalf("token calls = %d, want a re-mint near expiry", tokenCalls)
	}

	err := c.GetJSON(context.Background(), org, "vikn-projects", "/v1/issues/missing", nil, "project.issues.read")
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Status != http.StatusForbidden {
		t.Fatalf("expected a RemoteError 403, got %v", err)
	}
}

func TestClientReportsGrantRefusal(t *testing.T) {
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"access_denied","error_description":"no active app grant"}`))
	}))
	defer auth.Close()
	c := NewClient(Credentials{AuthURL: auth.URL, ClientID: "desk", ClientSecret: "s"})
	_, err := c.Resolve(context.Background(), uuid.New(), "vikn-projects", "project.issues.read")
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Status != http.StatusForbidden || !strings.Contains(remote.Body, "no active app grant") {
		t.Fatalf("err = %v", err)
	}
	var nilClient *Client
	if _, err := nilClient.Token(context.Background(), uuid.New(), "x", "s"); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("nil client: %v", err)
	}
	t.Setenv("AUTH_SERVICE_URL", "")
	if NewClientFromEnv() != nil {
		t.Fatal("no credentials must give a nil client")
	}
}
