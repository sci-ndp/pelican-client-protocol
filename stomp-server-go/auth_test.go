package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func writeHtpasswd(t *testing.T, entries map[string]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "htpasswd")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create htpasswd: %v", err)
	}
	defer f.Close()
	for user, pass := range entries {
		hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.MinCost)
		if err != nil {
			t.Fatalf("bcrypt: %v", err)
		}
		if _, err := f.WriteString(user + ":" + string(hash) + "\n"); err != nil {
			t.Fatalf("write htpasswd: %v", err)
		}
	}
	return path
}

func TestBasicAuthAcceptsCorrectCredentials(t *testing.T) {
	path := writeHtpasswd(t, map[string]string{"alice": "s3cret"})
	a, err := newBasicAuth(path, discardLogger())
	if err != nil {
		t.Fatalf("newBasicAuth: %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("alice", "s3cret")
	w := httptest.NewRecorder()
	if !a.Authenticate(w, r) {
		t.Fatal("expected Authenticate to accept correct credentials")
	}
}

func TestBasicAuthRejectsWrongPassword(t *testing.T) {
	path := writeHtpasswd(t, map[string]string{"alice": "s3cret"})
	a, err := newBasicAuth(path, discardLogger())
	if err != nil {
		t.Fatalf("newBasicAuth: %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("alice", "wrong")
	w := httptest.NewRecorder()
	if a.Authenticate(w, r) {
		t.Fatal("expected Authenticate to reject wrong password")
	}
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestBasicAuthRejectsMissingCredentials(t *testing.T) {
	path := writeHtpasswd(t, map[string]string{"alice": "s3cret"})
	a, err := newBasicAuth(path, discardLogger())
	if err != nil {
		t.Fatalf("newBasicAuth: %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	if a.Authenticate(w, r) {
		t.Fatal("expected Authenticate to reject a request with no credentials")
	}
}

func TestNewBasicAuthRejectsNonBcryptHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "htpasswd")
	// {SHA} is a legacy htpasswd format this server intentionally refuses.
	if err := os.WriteFile(path, []byte("alice:{SHA}qUqP5cyxm6YcTAhz05Hph5gvu9M=\n"), 0o600); err != nil {
		t.Fatalf("write htpasswd: %v", err)
	}
	if _, err := newBasicAuth(path, discardLogger()); err == nil {
		t.Fatal("expected newBasicAuth to reject a non-bcrypt hash")
	}
}

func TestNoAuthAcceptsEverything(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	if !(noAuth{}).Authenticate(w, r) {
		t.Fatal("expected noAuth to accept every request")
	}
}
