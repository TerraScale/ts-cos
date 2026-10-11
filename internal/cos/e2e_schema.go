package cos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// t3SchemaMarker starts the public message of a T3SchemaError. Bus.Call returns remote errors as text,
// so the host's error arrives here as a string and errors.As cannot see the type.
const t3SchemaMarker = "T3 schema changed on "

// checkT3Schema asks every host for its T3 shell through the COS micro service. A host whose T3 answers
// in a shape COS does not understand (after a T3 upgrade) fails the check.
func checkT3Schema(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	fleet, err := Fleet(ctx, bus)
	if err != nil {
		return "", err
	}
	return t3SchemaVerdict(fleet)
}

// t3SchemaVerdict fails on any host error and names the T3 shell schema when every host answers.
func t3SchemaVerdict(fleet []HostFleet) (string, error) {
	var hosts []string
	for _, f := range fleet {
		switch {
		case strings.HasPrefix(f.Error, t3SchemaMarker):
			return "", errors.New(f.Error)
		case f.Error != "":
			return "", fmt.Errorf("%s: %s", f.Host, f.Error)
		}
		hosts = append(hosts, f.Host)
	}
	slices.Sort(hosts)
	if want := slices.Sorted(slices.Values(e2eHosts)); !slices.Equal(hosts, want) {
		return "", fmt.Errorf("fleet answers for %v, want %v", hosts, want)
	}
	return fmt.Sprintf("T3 shell schema %d on %s", t3ShellSchema, strings.Join(hosts, ", ")), nil
}
