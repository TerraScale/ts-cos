package cos

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const dashboardUser = "cos"

func dashboardTokenPath(home string) string {
	return filepath.Join(home, ".config", "cos", "dashboard-token")
}

func loadDashboardToken(home string) (string, error) {
	path := dashboardTokenPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", fmt.Errorf("prepare dashboard credentials: %w", err)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", errors.New("generate dashboard credentials")
		}
		token := hex.EncodeToString(random[:])
		f, err := os.CreateTemp(filepath.Dir(path), ".dashboard-token-*")
		if err != nil {
			return "", fmt.Errorf("prepare dashboard credentials: %w", err)
		}
		tempPath := f.Name()
		defer os.Remove(tempPath)
		if err := f.Chmod(0600); err != nil {
			_ = f.Close()
			return "", fmt.Errorf("secure dashboard credentials: %w", err)
		}
		if _, err := f.WriteString(token + "\n"); err != nil {
			_ = f.Close()
			return "", errors.New("write dashboard credentials")
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return "", errors.New("sync dashboard credentials")
		}
		if err := f.Close(); err != nil {
			return "", errors.New("write dashboard credentials")
		}
		if err := os.Link(tempPath, path); errors.Is(err, os.ErrExist) {
			return loadDashboardToken(home)
		} else if err != nil {
			return "", fmt.Errorf("publish dashboard credentials: %w", err)
		}
		return token, nil
	}
	if err != nil {
		return "", fmt.Errorf("inspect dashboard credentials: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return "", errors.New("dashboard credentials must be a regular file with mode 0600")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read dashboard credentials: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", errors.New("dashboard credentials are empty or invalid")
	}
	return token, nil
}

func dashboardAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !trustedDashboardRemote(r.RemoteAddr) {
			http.Error(w, "Dashboard access is limited to loopback and Tailscale", http.StatusForbidden)
			return
		}
		user, password, ok := r.BasicAuth()
		userHash, expectedUserHash := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(dashboardUser))
		passwordHash, expectedPasswordHash := sha256.Sum256([]byte(password)), sha256.Sum256([]byte(token))
		userOK := subtle.ConstantTimeCompare(userHash[:], expectedUserHash[:]) == 1
		passOK := subtle.ConstantTimeCompare(passwordHash[:], expectedPasswordHash[:]) == 1
		if token == "" || !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="COS dashboard", charset="UTF-8"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func trustedDashboardRemote(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	_, tailscaleV4, _ := net.ParseCIDR("100.64.0.0/10")
	_, tailscaleV6, _ := net.ParseCIDR("fd7a:115c:a1e0::/48")
	return tailscaleV4.Contains(ip) || tailscaleV6.Contains(ip)
}
