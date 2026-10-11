package cos

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A self-change acceptance check is declarative so COS can write it before any code
// exists: GET a dashboard path on every node (signed in as cos-probe) or render a
// behavior template, then require or forbid substrings. The same check runs in CI
// against the PR build and on every deployed node after rollout.
type selfCheck struct {
	Summary  string   `json:"summary"`
	Path     string   `json:"path,omitempty"`
	Template string   `json:"template,omitempty"`
	Contains []string `json:"contains,omitempty"`
	Absent   []string `json:"absent,omitempty"`
}

type selfCheckResult struct {
	Host     string `json:"host"`
	Pass     bool   `json:"pass"`
	Evidence string `json:"evidence"`
}

const (
	selfDashboard = "http://127.0.0.1:8790" // every node serves the dashboard here
	selfNodes     = 3
	selfProbeTime = 20 * time.Second
	// Evidence when no process serves the node's selfcheck subject: its build predates TS-817.
	selfNotDeployed = "self-check probe not deployed"
)

var (
	selfPathPattern     = regexp.MustCompile(`^/[A-Za-z0-9/_.?=&%-]{0,200}$`)
	selfTemplatePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,40}$`)
)

func (c selfCheck) validate() error {
	switch {
	case strings.TrimSpace(c.Summary) == "" || len(c.Summary) > 160 || strings.Contains(c.Summary, "\n"):
		return errors.New("check summary must be one line of 1-160 characters")
	case (c.Path == "") == (c.Template == ""):
		return errors.New("check needs exactly one of path or template")
	case c.Path != "" && (!selfPathPattern.MatchString(c.Path) || strings.Contains(c.Path, "..") || strings.HasPrefix(c.Path, "//")):
		return errors.New("check path must be a plain dashboard path")
	case c.Template != "" && !selfTemplatePattern.MatchString(c.Template):
		return errors.New("check template must be a behavior template name")
	case len(c.Contains)+len(c.Absent) == 0 || len(c.Contains)+len(c.Absent) > 10:
		return errors.New("check needs 1-10 contains/absent strings")
	}
	for _, s := range slices.Concat(c.Contains, c.Absent) {
		if strings.TrimSpace(s) == "" || len(s) > 200 {
			return errors.New("check strings must be 1-200 characters")
		}
	}
	return nil
}

// judge applies the assertions to what a node served.
func (c selfCheck) judge(body string) error {
	for _, s := range c.Contains {
		if !strings.Contains(body, s) {
			return fmt.Errorf("missing %q", s)
		}
	}
	for _, s := range c.Absent {
		if strings.Contains(body, s) {
			return fmt.Errorf("still shows %q", s)
		}
	}
	return nil
}

// runLocal evaluates the check on this node: its live behavior or its own dashboard.
func (c selfCheck) runLocal(ctx context.Context, js jetstream.JetStream, l *BehaviorLoader) error {
	if c.Template != "" {
		b := l.Current()
		out, err := b.Render(c.Template, behaviorTemplates[c.Template])
		if err != nil {
			return fmt.Errorf("template %s (%s): %w", c.Template, b.Version, err)
		}
		return c.judge(out)
	}
	cookie, end, err := selfProbeCookie(ctx, js)
	if err != nil {
		return err
	}
	defer end()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, selfDashboard+c.Path, nil)
	if err != nil {
		return err
	}
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	client := http.Client{Timeout: selfProbeTime, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: dashboard unreachable", c.Path)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", c.Path, res.StatusCode)
	}
	return c.judge(string(body))
}

// selfProbeCookie signs in as the password-less "cos-probe" session user, like
// Auth.ProbeSession, from any process with COS NATS access; end deletes the session.
func selfProbeCookie(ctx context.Context, js jetstream.JetStream) (string, func(), error) {
	sessions, err := js.KeyValue(ctx, sessionBucket)
	if err != nil {
		return "", nil, fmt.Errorf("COS sessions unavailable: %w", err)
	}
	raw := make([]byte, 32)
	rand.Read(raw)
	token := base64.RawURLEncoding.EncodeToString(raw)
	key, _ := sessionKey("cos-probe", token)
	if _, err := sessions.Put(ctx, key, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return "", nil, err
	}
	return "cos-probe." + token, func() { _ = sessions.Delete(context.Background(), key) }, nil
}

func selfCheckSubject(host string) string { return "cos." + host + ".selfcheck" }

// runSelfProbe answers acceptance-check requests for this node until ctx ends.
func runSelfProbe(ctx context.Context, cfg Config, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		return
	}
	sub, err := bus.conn.Subscribe(selfCheckSubject(cfg.LocalHost), func(m *nats.Msg) {
		r := selfCheckResult{Host: cfg.LocalHost}
		var c selfCheck
		err := json.Unmarshal(m.Data, &c)
		if err == nil {
			err = c.validate()
		}
		if err == nil {
			run, cancel := context.WithTimeout(ctx, selfProbeTime)
			err = c.runLocal(run, js, behaviors)
			cancel()
		}
		r.Pass, r.Evidence = err == nil, "ok"
		if err != nil {
			r.Evidence = selfClip(err.Error())
		}
		data, _ := json.Marshal(r)
		_ = m.Respond(data)
	})
	if err != nil {
		return
	}
	<-ctx.Done()
	_ = sub.Unsubscribe()
}

func init() { serveWorkers = append(serveWorkers, runSelfProbe) }

// selfClip keeps evidence short enough for one chat line.
func selfClip(s string) string {
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// runSelfCheck asks every node to run the check. passed is true only when at least
// selfNodes nodes answered and all of them passed.
func runSelfCheck(ctx context.Context, bus *Bus, c selfCheck) (results []selfCheckResult, passed bool, err error) {
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return nil, false, err
	}
	data, _ := json.Marshal(c)
	results = make([]selfCheckResult, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Go(func() {
			results[i] = selfCheckResult{Host: host, Evidence: "no answer"}
			call, cancel := context.WithTimeout(ctx, selfProbeTime+5*time.Second)
			defer cancel()
			m, err := bus.conn.RequestWithContext(call, selfCheckSubject(host), data)
			switch {
			case err == nil:
				_ = json.Unmarshal(m.Data, &results[i])
			case errors.Is(err, nats.ErrNoResponders):
				results[i].Evidence = selfNotDeployed
			}
		})
	}
	wg.Wait()
	passed = len(results) >= selfNodes
	for _, r := range results {
		passed = passed && r.Pass
	}
	return results, passed, nil
}

// failedEverywhere is the revert and pre-work condition: every node answered and none passed.
func failedEverywhere(results []selfCheckResult) bool {
	if len(results) < selfNodes {
		return false
	}
	for _, r := range results {
		if r.Pass || r.Evidence == "no answer" || r.Evidence == selfNotDeployed {
			return false
		}
	}
	return true
}

func selfEvidence(results []selfCheckResult) string {
	parts := make([]string, 0, len(results))
	for _, r := range results {
		parts = append(parts, r.Host+": "+r.Evidence)
	}
	return strings.Join(parts, "; ")
}
