package cos

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInUnit(t *testing.T) {
	cases := []struct {
		cgroup string
		want   bool
	}{
		{"0::/user.slice/user-1000.slice/user@1000.service/app.slice/cos.service\n", true},
		{"0::/user.slice/user-1000.slice/session-3.scope\n", false},
	}
	old := procCgroup
	t.Cleanup(func() { procCgroup = old })
	for _, c := range cases {
		procCgroup = filepath.Join(t.TempDir(), "cgroup")
		if err := os.WriteFile(procCgroup, []byte(c.cgroup), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := inUnit(e2eUnit); got != c.want {
			t.Errorf("inUnit(%q) = %t, want %t", c.cgroup, got, c.want)
		}
	}
}

func TestSQLiteDiskFullCheck(t *testing.T) {
	got, err := checkSQLiteDiskFull(t.Context(), nil)
	if err != nil {
		t.Fatalf("checkSQLiteDiskFull: %v", err)
	}
	if !strings.Contains(got, "failed with") {
		t.Fatalf("checkSQLiteDiskFull evidence = %q; want the disk-full write", got)
	}
}

func TestCheckForgejo5xx(t *testing.T) {
	got, err := checkForgejo5xx(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}})
	if err != nil || !strings.HasPrefix(got, "503:") {
		t.Fatalf("checkForgejo5xx = %q, %v; want a 503 evidence line", got, err)
	}
	// A Forgejo that answers 200 from the start must fail the check, or it proves nothing.
	if got, err := forgejo5xxScenario(t.Context(), false); err == nil {
		t.Fatalf("healthy Forgejo passed the 5xx check with %q", got)
	}
}
