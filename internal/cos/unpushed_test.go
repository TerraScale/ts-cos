package cos

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gitTestRepo returns a clone of a bare remote with one pushed commit and a local git identity.
func gitTestRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	root := t.TempDir()
	remote, clone := filepath.Join(root, "remote.git"), filepath.Join(root, "clone")
	run := func(dir string, args ...string) {
		t.Helper()
		if out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run(root, "init", "--bare", remote)
	run(root, "clone", remote, clone)
	run(clone, "config", "user.name", "test")
	run(clone, "config", "user.email", "test@example.com")
	run(clone, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(clone, "a.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(clone, "add", "a.txt")
	run(clone, "commit", "-m", "init")
	run(clone, "push", "-u", "origin", "HEAD")
	return clone
}

func TestWorktreeStateReportsDirtyAndAhead(t *testing.T) {
	clone := gitTestRepo(t)
	ctx := context.Background()
	check := func(step string, wantDirty bool, wantAhead int) {
		t.Helper()
		dirty, ahead, err := worktreeState(ctx, clone)
		if err != nil {
			t.Fatal(step, err)
		}
		if dirty != wantDirty || ahead != wantAhead {
			t.Fatalf("%s: dirty=%v ahead=%d, want dirty=%v ahead=%d", step, dirty, ahead, wantDirty, wantAhead)
		}
	}
	check("clean and pushed", false, 0)

	if out, err := exec.Command("git", "-C", clone, "commit", "--allow-empty", "-m", "local").CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	check("one local commit", false, 1)

	if err := os.WriteFile(filepath.Join(clone, "new.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	check("untracked file", true, 1)

	if err := os.WriteFile(filepath.Join(clone, "a.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	check("modified file", true, 1)
}

func TestWorktreeStateFailsOutsideRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	if _, _, err := worktreeState(context.Background(), t.TempDir()); err == nil {
		t.Fatal("accepted a directory that is not a git repo")
	}
}

func TestUnpushedTakesOneThreadID(t *testing.T) {
	if err := validateLocal("unpushed", []string{"thread-1"}); err != nil {
		t.Fatal(err)
	}
	if err := validateLocal("unpushed", nil); err == nil {
		t.Fatal("accepted a missing thread id")
	}
}

func TestUnpushedStateCoversWorktreeCases(t *testing.T) {
	ctx := context.Background()
	state, err := unpushedState(ctx, "h", "t1", "")
	if err != nil || state["dirty"] != false || state["ahead"] != 0 {
		t.Fatalf("empty path: %#v, %v", state, err)
	}
	if _, ok := state["missing"]; ok {
		t.Fatal("empty path reported missing")
	}

	state, err = unpushedState(ctx, "h", "t1", filepath.Join(t.TempDir(), "gone"))
	if err != nil || state["missing"] != true {
		t.Fatalf("missing path: %#v, %v", state, err)
	}

	clone := gitTestRepo(t)
	if out, err := exec.Command("git", "-C", clone, "commit", "--allow-empty", "-m", "local").CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	state, err = unpushedState(ctx, "h", "t1", clone)
	if err != nil || state["ahead"] != 1 || state["dirty"] != false {
		t.Fatalf("one local commit: %#v, %v", state, err)
	}

	if err := os.WriteFile(filepath.Join(clone, "new.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	state, err = unpushedState(ctx, "h", "t1", clone)
	if err != nil || state["dirty"] != true {
		t.Fatalf("dirty repo: %#v, %v", state, err)
	}
}

func TestGitOutputNamesStderr(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	_, err := gitOutput(context.Background(), t.TempDir(), "status")
	if err == nil || !strings.Contains(err.Error(), "not a git repository") {
		t.Fatalf("err = %v, want git stderr", err)
	}
}

func TestShellCopiesThreadLastActivity(t *testing.T) {
	snapshot := fmt.Sprintf(`{"schemaVersion":%d,"threads":[{"id":"t1","projectId":"p1","title":"TS-829 x","status":"completed","updatedAt":"2026-10-11T10:00:00Z"}]}`, t3ShellSchema)
	server := newT3TestServer(t, snapshot, func(string, map[string]any) any { return nil })
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL}
	fleet, err := client.Shell(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet.Threads) != 1 || fleet.Threads[0].Updated != "2026-10-11T10:00:00Z" {
		t.Fatalf("threads = %#v", fleet.Threads)
	}
}
