package cos

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// checkHomeKPIs signs in on base and checks the chat-first home renders the KPI tiles.
func checkHomeKPIs(ctx context.Context, base string, u *e2eUser) (string, error) {
	cookie, status, _, err := signIn(ctx, base, u.name, u.password)
	if err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("login on %s: HTTP %d %v", base, status, err)
	}
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/", cookie, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK || !strings.Contains(body, `id="kpis"`) || strings.Count(body, `class="kpi"`) != 6 {
		return "", fmt.Errorf("GET %s/: HTTP %d without six KPI tiles", base, status)
	}
	return "home on " + base + " renders six KPI tiles", nil
}
