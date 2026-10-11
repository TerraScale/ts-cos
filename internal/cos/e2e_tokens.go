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

// savedShare matches the savings card's summary row; the target is checked separately.
var savedShare = regexp.MustCompile(`Saved (\d+)% of spend`)

// checkTokensSavings uses the run's session on base and checks the Tokens page shows the savings card, and that host's savings ledger has today's row.
func checkTokensSavings(ctx context.Context, e *E2EEnv, host, base string) (string, error) {
	cookie, err := e.session(ctx, base)
	if err != nil {
		return "", err
	}
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/tokens?w=7d", cookie, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("GET %s/tokens?w=7d: HTTP %d", base, status)
	}
	pct, err := tokensSavingsPage(body)
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
	return fmt.Sprintf("/tokens on %s: saved %d%% of spend vs target %d%%; ledger today: %d tokens avoided across %d kinds",
		host, pct, savingsTargetPercent, avoided, kinds), nil
}

// tokensSavingsPage reads the saved share from a rendered /tokens page and checks the savings card has its rows and target.
func tokensSavingsPage(body string) (int, error) {
	for _, row := range []string{"Memory retrieval", "Fresh-brief restarts"} {
		if !strings.Contains(body, row) {
			return 0, fmt.Errorf("savings card has no %q row", row)
		}
	}
	m := savedShare.FindStringSubmatch(body)
	if m == nil {
		return 0, errors.New(`savings card has no "Saved N% of spend" row`)
	}
	if target := fmt.Sprintf("target %d%%", savingsTargetPercent); !strings.Contains(body, target) {
		return 0, fmt.Errorf("savings card has no %q", target)
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
