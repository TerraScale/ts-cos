package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// haikuShare matches the Haiku share KPI on the Tokens page.
var haikuShare = regexp.MustCompile(`Haiku share</span>\s*<span class="kpi-value">(\d+)%`)

// checkTokensSavings uses the run's session on base and checks the Tokens page shows the Haiku share, and that host's savings ledger has today's row.
func checkTokensSavings(ctx context.Context, e *E2EEnv, host, base string) (string, error) {
	cookie, end, err := e.ownerSession(ctx)
	if err != nil {
		return "", err
	}
	defer end()
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/tokens?w=7d", cookie, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("GET %s/tokens?w=7d: HTTP %d", base, status)
	}
	pct, err := tokensHaikuShare(body)
	if err != nil {
		return "", fmt.Errorf("GET %s/tokens?w=7d: %w", base, err)
	}
	out, err := e.Host(ctx, host, "~/.local/bin/cos savings 1")
	if err != nil {
		return "", fmt.Errorf("cos savings on %s: %w", host, err)
	}
	today := time.Now().UTC().Format("2006-01-02")
	avoided, kinds, err := ledgerToday(out, today)
	if err != nil {
		return "", fmt.Errorf("cos savings on %s: %w", host, err)
	}
	return fmt.Sprintf("/tokens on %s: Haiku share %d%%; ledger today: %d tokens avoided across %d kinds",
		host, pct, avoided, kinds), nil
}

// tokensHaikuShare reads the Haiku share KPI from a rendered /tokens page.
func tokensHaikuShare(body string) (int, error) {
	m := haikuShare.FindStringSubmatch(body)
	if m == nil {
		return 0, errors.New(`Tokens page has no "Haiku share" KPI`)
	}
	return strconv.Atoi(m[1])
}

// ledgerToday reads today's row from `cos savings` output: tokens avoided and how many kinds recorded them.
func ledgerToday(out, today string) (int, int, error) {
	var r struct {
		Days []SavingsDay `json:"days"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &r); err != nil {
		return 0, 0, fmt.Errorf("savings prints no JSON: %s", e2eClip(out, 80))
	}
	i := slices.IndexFunc(r.Days, func(d SavingsDay) bool { return d.Day == today })
	if i < 0 {
		return 0, 0, fmt.Errorf("savings has no row for %s", today)
	}
	return r.Days[i].Avoided, len(r.Days[i].Kinds), nil
}
