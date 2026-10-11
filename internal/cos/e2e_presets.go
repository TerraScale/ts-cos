package cos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// checkHeartbeatPresets signs in on one node and checks the Settings page offers every heartbeat preset
// and that the stored interval is one of them. It reads the preset and never sets it.
func checkHeartbeatPresets(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	value, found, err := storedInterval(ctx, bus)
	if err != nil {
		return "", err
	}
	interval, err := intervalSetting(value, found)
	if err != nil {
		return "", err
	}
	urls, err := e.nodeURLs(ctx)
	if err != nil {
		return "", err
	}
	return presetPage(ctx, e, urls[0], interval)
}

// storedInterval reads the interval key the autopilot runs on; found is false when no value is stored.
func storedInterval(ctx context.Context, bus *Bus) ([]byte, bool, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, false, err
	}
	kv, err := js.KeyValue(ctx, autopilotSettings)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	entry, err := kv.Get(ctx, "interval")
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	return entry.Value(), true, nil
}

// intervalSetting parses the stored interval, which must be a preset; no stored value means the default.
func intervalSetting(value []byte, found bool) (int, error) {
	if !found {
		return autopilotDefault, nil
	}
	n, err := strconv.Atoi(string(value))
	if err != nil || !slices.Contains(autopilotPresets, n) {
		return 0, fmt.Errorf("stored interval %q is not one of the presets %v", e2eClip(string(value), 40), autopilotPresets)
	}
	return n, nil
}

// presetButtonRE matches a rendered heartbeat preset button: its aria-pressed state and its minutes.
var presetButtonRE = regexp.MustCompile(`aria-pressed="(true|false)"[^>]*>(\d+) min</button>`)

// presetButtons maps each rendered preset to whether it is pressed.
func presetButtons(body string) map[int]bool {
	out := map[int]bool{}
	for _, m := range presetButtonRE.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[2])
		out[n] = m[1] == "true"
	}
	return out
}

// presetPage checks GET /settings on base renders every preset.
func presetPage(ctx context.Context, e *E2EEnv, base string, interval int) (string, error) {
	cookie, err := e.session(ctx, base)
	if err != nil {
		return "", err
	}
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/settings", cookie, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("GET %s/settings: HTTP %d, want 200", base, status)
	}
	rendered := presetButtons(body)
	var missing []int
	for _, m := range autopilotPresets {
		if _, ok := rendered[m]; !ok {
			missing = append(missing, m)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("GET %s/settings does not render the presets %v", base, missing)
	}
	return fmt.Sprintf("settings on %s render presets %v; stored interval %d min is one of them", base, autopilotPresets, interval), nil
}
