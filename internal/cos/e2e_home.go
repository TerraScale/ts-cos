package cos

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// checkHomeKPIs uses the run's session on base and checks the chat-first home renders the KPI tiles.
func checkHomeKPIs(ctx context.Context, e *E2EEnv, base string) (string, error) {
	cookie, end, err := e.ownerSession(ctx)
	if err != nil {
		return "", err
	}
	defer end()
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/", cookie, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK || !strings.Contains(body, `id="kpis"`) || strings.Count(body, `class="kpi"`) != 6 {
		return "", fmt.Errorf("GET %s/: HTTP %d without six KPI tiles", base, status)
	}
	return "home on " + base + " renders six KPI tiles", nil
}

// checkKPIsLive publishes a kpi.probe and checks that base's open /chat/events stream repaints #kpis with it within 2 s.
func checkKPIsLive(ctx context.Context, e *E2EEnv, host, base string) (string, error) {
	cookie, end, err := e.ownerSession(ctx)
	if err != nil {
		return "", err
	}
	defer end()
	stream, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(stream, "GET", base+"/chat/events", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s/chat/events: %w", base, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s/chat/events: HTTP %d", base, res.StatusCode)
	}
	sc := newSSEScanner(res.Body)
	if !scanFor(sc, `id="kpis"`) {
		return "", fmt.Errorf("no initial #kpis repaint on %s/chat/events%s", base, scanErr(sc))
	}
	bus, err := ConnectBus(ctx, e.cfg)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	runner, now := e.LocalHost(), time.Now()
	id := NewEventID(SourceCOS, "kpi-probe", runner, strconv.FormatInt(now.UnixNano(), 10))
	if _, err := PublishEvent(ctx, js, Event{ID: id, Source: SourceCOS, Kind: KindKPIProbe, Time: now, Severity: SeverityInfo,
		Summary: "KPI liveness probe", Host: runner, Synthetic: true}); err != nil {
		return "", err
	}
	// The timer cancels the stream, so a probe that is not repainted in time ends the scan.
	deadline := time.AfterFunc(2*time.Second, cancel)
	defer deadline.Stop()
	seen := scanFor(sc, `data-probe="`+id+`"`)
	elapsed := time.Since(now)
	switch {
	case !seen:
		return "", fmt.Errorf("probe from %s not seen on %s within 2 s%s", runner, base, scanErr(sc))
	case elapsed > 2*time.Second:
		return "", fmt.Errorf("probe from %s seen on %s after %d ms, want within 2 s", runner, base, elapsed.Milliseconds())
	}
	return fmt.Sprintf("probe from %s on %s after %d ms", runner, host, elapsed.Milliseconds()), nil
}

// newSSEScanner reads an event stream line by line; one patch is one line and the chat history repaint exceeds the 64 KB default.
func newSSEScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	return sc
}

// scanFor reports whether a line from sc contains marker before the input ends.
func scanFor(sc *bufio.Scanner, marker string) bool {
	for sc.Scan() {
		if strings.Contains(sc.Text(), marker) {
			return true
		}
	}
	return false
}

// scanErr explains a failed scan with the scanner error, unless the probe deadline cancelled the stream.
func scanErr(sc *bufio.Scanner) string {
	if err := sc.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Sprintf(" (scan: %v)", err)
	}
	return ""
}
