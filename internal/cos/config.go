package cos

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type Config struct {
	Home, LocalHost, ForgejoURL, KuURL string
	HTTP                               *http.Client
	SkipReviewer                       bool
}

func DefaultConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, err
	}
	host, err := os.Hostname()
	if err != nil {
		return Config{}, err
	}
	return Config{Home: home, LocalHost: host, ForgejoURL: "https://git.terrascale.tech/api/v1", KuURL: "https://ku.mariogk.com/mcp", HTTP: &http.Client{Timeout: 60 * time.Second}}, nil
}

func validHost(host string) bool {
	if host == "" || strings.HasPrefix(host, "-") {
		return false
	}
	for _, c := range host {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_", c) {
			return false
		}
	}
	return host != "." && host != ".."
}

func uuid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

func objectJSON(raw string) (map[string]any, error) {
	var v map[string]any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil, fmt.Errorf("JSON object: %w", err)
	}
	if v == nil {
		return nil, fmt.Errorf("expected a JSON object")
	}
	return v, nil
}

func active(status string) bool {
	switch status {
	case "preparing", "queued", "starting", "running", "waiting":
		return true
	}
	return false
}
