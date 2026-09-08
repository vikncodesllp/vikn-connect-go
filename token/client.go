package token

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrNotConfigured means this process has no confidential client: set
// AUTH_SERVICE_URL, CONNECT_CLIENT_ID and CONNECT_CLIENT_SECRET.
var ErrNotConfigured = errors.New("vikn connect client credentials are not configured")

// Credentials identify this app to auth_go.
type Credentials struct {
	AuthURL      string
	ClientID     string
	ClientSecret string
}

// CredentialsFromEnv reads AUTH_SERVICE_URL, CONNECT_CLIENT_ID and
// CONNECT_CLIENT_SECRET. ok is false when any is missing.
func CredentialsFromEnv() (Credentials, bool) {
	c := Credentials{
		AuthURL:      strings.TrimRight(strings.TrimSpace(os.Getenv("AUTH_SERVICE_URL")), "/"),
		ClientID:     strings.TrimSpace(os.Getenv("CONNECT_CLIENT_ID")),
		ClientSecret: strings.TrimSpace(os.Getenv("CONNECT_CLIENT_SECRET")),
	}
	return c, c.AuthURL != "" && c.ClientID != "" && c.ClientSecret != ""
}

// Resolution is auth_go's answer to "where does this org's install of that
// app live, and what may I ask it".
type Resolution struct {
	AppSlug    string   `json:"app_slug"`
	APIBaseURL string   `json:"api_base_url"`
	APIVersion string   `json:"api_version"`
	Region     string   `json:"region"`
	Scopes     []string `json:"scopes"`
	EventTypes []string `json:"event_types"`
}

// RemoteError is a non-2xx answer from auth_go or a far-side app.
type RemoteError struct {
	Status int
	Body   string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote returned %d: %s", e.Status, strings.TrimSpace(e.Body))
}

// Client mints org-scoped integration tokens, resolves apps, and calls
// their scoped APIs. Tokens and resolutions are cached per org and app.
type Client struct {
	creds Credentials
	http  *http.Client
	now   func() time.Time

	mu          sync.Mutex
	tokens      map[string]cachedToken
	resolutions map[string]cachedResolution
}

type cachedToken struct {
	value   string
	expires time.Time
}

type cachedResolution struct {
	value   Resolution
	expires time.Time
}

const resolutionTTL = 5 * time.Minute

func NewClient(creds Credentials) *Client {
	return &Client{creds: creds, http: &http.Client{Timeout: 10 * time.Second}, now: time.Now,
		tokens: map[string]cachedToken{}, resolutions: map[string]cachedResolution{}}
}

// NewClientFromEnv returns nil when the credentials are not set, so a box
// without them degrades to "not available" rather than failing to start.
func NewClientFromEnv() *Client {
	creds, ok := CredentialsFromEnv()
	if !ok {
		return nil
	}
	return NewClient(creds)
}

func cacheKey(org uuid.UUID, audience string, scopes []string) string {
	sorted := append([]string(nil), scopes...)
	sort.Strings(sorted)
	return org.String() + "|" + audience + "|" + strings.Join(sorted, " ")
}

// Token returns an access token for this org's install of audience with the
// given scopes, reusing a cached one until a minute before it expires.
func (c *Client) Token(ctx context.Context, org uuid.UUID, audience string, scopes ...string) (string, error) {
	if c == nil {
		return "", ErrNotConfigured
	}
	if len(scopes) == 0 {
		return "", errors.New("at least one scope is required")
	}
	key := cacheKey(org, audience, scopes)
	c.mu.Lock()
	if cached, ok := c.tokens[key]; ok && c.now().Before(cached.expires) {
		c.mu.Unlock()
		return cached.value, nil
	}
	c.mu.Unlock()

	form := url.Values{"grant_type": {"client_credentials"}, "organization_id": {org.String()}, "audience": {audience}, "scope": {strings.Join(scopes, " ")}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.creds.AuthURL+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(c.creds.ClientID, c.creds.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusOK {
		return "", &RemoteError{Status: resp.StatusCode, Body: string(body)}
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.AccessToken == "" {
		return "", errors.New("token response had no access_token")
	}
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	c.mu.Lock()
	c.tokens[key] = cachedToken{value: payload.AccessToken, expires: c.now().Add(ttl - time.Minute)}
	c.mu.Unlock()
	return payload.AccessToken, nil
}

// Resolve asks auth_go where this org's install of app lives. It needs a
// token for that app, so the caller's grant on it is checked as a side
// effect: no grant, no address.
func (c *Client) Resolve(ctx context.Context, org uuid.UUID, app string, scopes ...string) (Resolution, error) {
	if c == nil {
		return Resolution{}, ErrNotConfigured
	}
	key := org.String() + "|" + app
	c.mu.Lock()
	if cached, ok := c.resolutions[key]; ok && c.now().Before(cached.expires) {
		c.mu.Unlock()
		return cached.value, nil
	}
	c.mu.Unlock()

	bearer, err := c.Token(ctx, org, app, scopes...)
	if err != nil {
		return Resolution{}, err
	}
	endpoint := c.creds.AuthURL + "/api/connect/resolve?" + url.Values{"app": {app}, "org_id": {org.String()}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Resolution{}, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return Resolution{}, fmt.Errorf("resolve request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusOK {
		return Resolution{}, &RemoteError{Status: resp.StatusCode, Body: string(body)}
	}
	var envelope struct {
		Data Resolution `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Data.APIBaseURL == "" {
		return Resolution{}, errors.New("resolve response had no api_base_url")
	}
	envelope.Data.APIBaseURL = strings.TrimRight(envelope.Data.APIBaseURL, "/")
	c.mu.Lock()
	c.resolutions[key] = cachedResolution{value: envelope.Data, expires: c.now().Add(resolutionTTL)}
	c.mu.Unlock()
	return envelope.Data, nil
}

// Do resolves app for org, mints a token with scopes, and performs the
// request against the app's base URL. path starts with a slash and is
// relative to the base URL (e.g. "/v1/issues/…"). The caller closes the body.
func (c *Client) Do(ctx context.Context, org uuid.UUID, app, method, path string, body io.Reader, scopes ...string) (*http.Response, error) {
	if c == nil {
		return nil, ErrNotConfigured
	}
	resolution, err := c.Resolve(ctx, org, app, scopes...)
	if err != nil {
		return nil, err
	}
	bearer, err := c.Token(ctx, org, app, scopes...)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, resolution.APIBaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

// GetJSON is Do for a GET whose JSON body is decoded into out. A non-2xx
// answer is returned as a RemoteError with the body preserved.
func (c *Client) GetJSON(ctx context.Context, org uuid.UUID, app, path string, out any, scopes ...string) error {
	resp, err := c.Do(ctx, org, app, http.MethodGet, path, nil, scopes...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &RemoteError{Status: resp.StatusCode, Body: string(raw)}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}
