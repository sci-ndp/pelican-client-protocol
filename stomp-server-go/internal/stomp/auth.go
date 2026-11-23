package stomp

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Principal identifies who an Authenticator admitted. It is established once,
// at the HTTP upgrade, and stays fixed for the life of the resulting STOMP
// connection; every server callback receives it so an application layer can
// decide what that caller may do (e.g. which destinations it may subscribe
// to). The zero value is the anonymous principal.
type Principal struct {
	Name string

	// Data carries whatever extra identity an Authenticator wants to hand to
	// an Authorizer or other callback (roles, claims, a user record, ...). It
	// is opaque to this package; the Authenticator and the code consuming the
	// Principal agree on its concrete type. Note that a Principal holding
	// non-comparable Data (e.g. a map) must not be compared with ==.
	Data any

	// Expiry, if non-zero, is when the Principal's credentials stop being
	// valid (e.g. the "exp" claim of a parsed JWT). The server closes the
	// connection at that instant. The zero value means no expiry.
	Expiry time.Time
}

// Anonymous reports whether p carries no authenticated identity.
func (p Principal) Anonymous() bool { return p.Name == "" }

// Authenticator decides whether an incoming HTTP request may proceed to the
// WebSocket upgrade. Authenticate is checked once per HTTP request, before
// any STOMP frames are exchanged. On success it returns who was admitted. On
// rejection it is responsible for writing an appropriate response (e.g. a 401
// with a WWW-Authenticate challenge) itself; the caller must not write to w
// if Authenticate returns false.
type Authenticator interface {
	Authenticate(w http.ResponseWriter, r *http.Request) (Principal, bool)
}

// Authorizer decides whether an authenticated Principal may subscribe to a
// destination. It is the policy half of access control (Authenticator being
// the identity half) and is consulted for every SUBSCRIBE; returning false
// rejects it with an ERROR frame and closes the connection. Implementations
// are called from connection goroutines concurrently, so they must be safe
// for concurrent use.
type Authorizer interface {
	Authorize(p Principal, destination string) bool
}

// AllowAll is an Authorizer that permits every subscription.
type AllowAll struct{}

func (AllowAll) Authorize(Principal, string) bool { return true }

type principalKey struct{}

// PrincipalFromContext returns the Principal RequireAuth stored in ctx, or
// the anonymous Principal if there is none.
func PrincipalFromContext(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey{}).(Principal)
	return p
}

// RequireAuth wraps next so every request is checked against a before being
// passed through. The admitted Principal is attached to the request's context
// (see PrincipalFromContext).
func RequireAuth(a Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := a.Authenticate(w, r)
		if !ok {
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

// NoAuth is an Authenticator that admits every request as the anonymous
// Principal.
type NoAuth struct{}

func (NoAuth) Authenticate(http.ResponseWriter, *http.Request) (Principal, bool) {
	return Principal{}, true
}

// basicAuth is an Authenticator backed by an htpasswd file, checked via HTTP
// Basic auth. Only bcrypt ($2a$/$2b$/$2y$, produced by `htpasswd -B`) entries
// are accepted; htpasswd's own tooling steers users toward bcrypt as the
// modern default, so legacy formats (MD5-crypt, {SHA}, crypt, plain) are
// rejected when the file loads rather than silently treated as an
// unmatchable password.
type basicAuth struct {
	realm     string
	passwords map[string]string // username -> bcrypt hash
	log       *slog.Logger
}

func NewBasicAuth(htpasswdPath string, log *slog.Logger) (*basicAuth, error) {
	f, err := os.Open(htpasswdPath)
	if err != nil {
		return nil, fmt.Errorf("open htpasswd file: %w", err)
	}
	defer f.Close()

	passwords := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		user, hash, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("htpasswd file: malformed line: %q", line)
		}
		if !isBcryptHash(hash) {
			return nil, fmt.Errorf("htpasswd file: unsupported hash format for user %q (only bcrypt is supported; regenerate with `htpasswd -B`)", user)
		}
		passwords[user] = hash
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read htpasswd file: %w", err)
	}
	return &basicAuth{realm: "stomp-playground", passwords: passwords, log: log}, nil
}

func isBcryptHash(hash string) bool {
	for _, prefix := range []string{"$2a$", "$2b$", "$2y$"} {
		if strings.HasPrefix(hash, prefix) {
			return true
		}
	}
	return false
}

func (a *basicAuth) Authenticate(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	user, pass, ok := r.BasicAuth()
	if ok && a.check(user, pass) {
		a.log.Debug("basic auth accepted", "user", user, "remote_addr", r.RemoteAddr)
		return Principal{Name: user}, true
	}
	a.log.Debug("basic auth rejected", "user", user, "remote_addr", r.RemoteAddr)
	w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", a.realm))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return Principal{}, false
}

func (a *basicAuth) check(user, pass string) bool {
	hash, ok := a.passwords[user]
	if !ok {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pass)) == nil
}
