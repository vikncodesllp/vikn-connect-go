// Package token is the credential side of Vikn Connect: verifying the
// org-scoped integration tokens auth_go mints from the client-credentials
// grant, and obtaining them. Every app that exposes or calls a scoped API
// uses this instead of its own copy of the JWKS cache and claims checks.
package token

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	// Issuer is the iss claim on every token auth_go signs.
	Issuer = "vikn-auth"
	// TokenUseIntegration is the token_use of a client-credentials token.
	TokenUseIntegration = "integration"
	// DefaultJWKSURL is production auth_go's key set.
	DefaultJWKSURL = "https://auth.vikn.io/.well-known/jwks.json"

	jwksRefreshMinInterval = 30 * time.Second
)

// Claims are workload credentials minted by auth_go's client-credentials
// grant, deliberately distinct from a person's claims so an API cannot
// confuse the two.
type Claims struct {
	ClientID       string    `json:"client_id"`
	OrganizationID uuid.UUID `json:"org_id"`
	Scope          string    `json:"scope"`
	TokenUse       string    `json:"token_use"`
	jwt.RegisteredClaims
}

// Scopes splits the space-separated scope claim.
func (c *Claims) Scopes() []string { return strings.Fields(c.Scope) }

// HasScope reports whether the token was granted scope.
func (c *Claims) HasScope(scope string) bool {
	for _, granted := range c.Scopes() {
		if granted == scope {
			return true
		}
	}
	return false
}

// Verifier checks RS256 integration tokens against a JWKS endpoint, caching
// keys by kid and refreshing on an unknown kid (rate-limited, so a flood of
// bad tokens cannot hammer auth_go).
type Verifier struct {
	jwksURL string
	client  *http.Client

	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey
	lastRefresh time.Time
}

func NewVerifier(jwksURL string) *Verifier {
	return &Verifier{jwksURL: jwksURL, client: &http.Client{Timeout: 5 * time.Second}, keys: map[string]*rsa.PublicKey{}}
}

// JWKSURLFromEnv is the key set this process is federated with:
// SSO_JWKS_URL, else AUTH_SERVICE_URL + /.well-known/jwks.json, else
// production. Sandbox and production advertise the same kid with different
// keys, so a sandbox app must point at its own auth_go.
func JWKSURLFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("SSO_JWKS_URL")); value != "" {
		return value
	}
	if base := strings.TrimRight(strings.TrimSpace(os.Getenv("AUTH_SERVICE_URL")), "/"); base != "" {
		return base + "/.well-known/jwks.json"
	}
	return DefaultJWKSURL
}

// VerifierFromEnv is NewVerifier(JWKSURLFromEnv()).
func VerifierFromEnv() *Verifier { return NewVerifier(JWKSURLFromEnv()) }

// Verify parses and checks an integration token: RS256 against the JWKS,
// issuer vikn-auth, the audience when one is given (an empty audience
// accepts any, for registry endpoints every app may call), token_use
// integration, and every required scope.
func (v *Verifier) Verify(raw, audience string, requiredScopes ...string) (*Claims, error) {
	claims := &Claims{}
	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}), jwt.WithIssuer(Issuer)}
	if audience != "" {
		opts = append(opts, jwt.WithAudience(audience))
	}
	parsed, err := jwt.ParseWithClaims(strings.TrimSpace(raw), claims, v.keyfunc, opts...)
	if err != nil || !parsed.Valid {
		return nil, errors.New("invalid integration token")
	}
	if claims.TokenUse != TokenUseIntegration || claims.ClientID == "" || claims.OrganizationID == uuid.Nil || claims.Subject == "" {
		return nil, errors.New("token is not an integration credential")
	}
	for _, required := range requiredScopes {
		if !claims.HasScope(required) {
			return nil, errors.New("required scope is missing: " + required)
		}
	}
	return claims, nil
}

func (v *Verifier) keyfunc(t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	if key := v.lookup(kid); key != nil {
		return key, nil
	}
	if err := v.refresh(); err != nil {
		return nil, err
	}
	if key := v.lookup(kid); key != nil {
		return key, nil
	}
	return nil, errors.New("no verification key for kid " + kid)
}

func (v *Verifier) lookup(kid string) *rsa.PublicKey {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.keys[kid]
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (v *Verifier) refresh() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if time.Since(v.lastRefresh) < jwksRefreshMinInterval && len(v.keys) > 0 {
		return nil
	}
	resp, err := v.client.Get(v.jwksURL)
	if err != nil {
		return fmt.Errorf("fetch JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned %s", resp.Status)
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("decode JWKS: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pub, err := rsaPublicKey(k.N, k.E)
		if err != nil {
			return err
		}
		keys[k.Kid] = pub
	}
	v.keys, v.lastRefresh = keys, time.Now()
	return nil
}

func rsaPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: int(new(big.Int).SetBytes(eBytes).Int64())}, nil
}

// BearerFromHeader extracts the token from an Authorization header.
func BearerFromHeader(header string) string {
	raw, ok := strings.CutPrefix(strings.TrimSpace(header), "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(raw)
}
