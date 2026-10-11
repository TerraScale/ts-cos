package cos

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeVault struct {
	stored    []byte
	reads     int
	visibleAt int // read count from which a created secret is visible
	readErr   error
	createErr error
}

func (f *fakeVault) read(context.Context, string) ([]byte, error) {
	f.reads++
	if f.readErr != nil {
		return nil, f.readErr
	}
	if f.stored == nil || f.reads < f.visibleAt {
		return nil, errVaultMissing
	}
	return bytes.Clone(f.stored), nil
}

func (f *fakeVault) create(_ context.Context, _ string, content []byte) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.stored = bytes.Clone(content)
	return nil
}

func TestVaultMaster(t *testing.T) {
	ctx := context.Background()
	t.Run("first boot creates a random master and reads it back", func(t *testing.T) {
		f := &fakeVault{visibleAt: 3}
		master, err := vaultMaster(ctx, f)
		if err != nil || len(master) != 32 || !bytes.Equal(master, f.stored) || bytes.Equal(master, make([]byte, 32)) {
			t.Fatalf("master %x, %v", master, err)
		}
	})
	t.Run("a node losing the creation race reads the winner's master", func(t *testing.T) {
		winner := bytes.Repeat([]byte{7}, 32)
		f := &fakeVault{createErr: errors.New("OCI Vault create of cos-master: HTTP 400 InvalidParameter")}
		f.visibleAt = 2
		f.stored = winner
		master, err := vaultMaster(ctx, f)
		if err != nil || !bytes.Equal(master, winner) {
			t.Fatalf("master %x, %v", master, err)
		}
	})
	t.Run("read failures and bad content stop startup without content", func(t *testing.T) {
		if _, err := vaultMaster(ctx, &fakeVault{readErr: errors.New("OCI Vault read of cos-master: HTTP 401 NotAuthenticated")}); err == nil {
			t.Fatal("read failure ignored")
		}
		_, err := vaultMaster(ctx, &fakeVault{stored: []byte("PRIVATE-short")})
		if err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("short master: %v", err)
		}
	})
	t.Run("derived credentials are stable and separated by purpose", func(t *testing.T) {
		master := bytes.Repeat([]byte{1}, 32)
		route, client := derive(master, "nats route"), derive(master, "nats client")
		if route == client || len(route) != 64 || route != derive(master, "nats route") || route == derive(bytes.Repeat([]byte{2}, 32), "nats route") {
			t.Fatal("derived credentials are not stable, separated and secret-dependent")
		}
	})
}
