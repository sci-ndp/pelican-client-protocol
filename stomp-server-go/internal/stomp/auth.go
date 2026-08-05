package stomp

import (
	"bufio"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Authenticator decides whether an incoming HTTP request may proceed to the
// WebSocket upgrade. Authenticate is checked once per HTTP request, before
// any STOMP frames are exchanged. On rejection it is responsible for writing
// an appropriate response (e.g. a 401 with a WWW-Authenticate challenge)
// itself; the caller must not write to w if Authenticate returns false.
type Authenticator interface {
	Authenticate(w http.ResponseWriter, r *http.Request) bool
}

// RequireAuth wraps next so every request is checked against a before being
// passed through.
func RequireAuth(a Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.Authenticate(w, r) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

// NoAuth is an Authenticator that admits every request.
type NoAuth struct{}

func (NoAuth) Authenticate(http.ResponseWriter, *http.Request) bool { return true }

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

func (a *basicAuth) Authenticate(w http.ResponseWriter, r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	if ok && a.check(user, pass) {
		a.log.Debug("basic auth accepted", "user", user, "remote_addr", r.RemoteAddr)
		return true
	}
	a.log.Debug("basic auth rejected", "user", user, "remote_addr", r.RemoteAddr)
	w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", a.realm))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func (a *basicAuth) check(user, pass string) bool {
	hash, ok := a.passwords[user]
	if !ok {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pass)) == nil
}
