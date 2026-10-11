package cos

import (
	"context"
	"errors"
	"io"
	"runtime/debug"
)

// releaseProtocol is what preflight requires of a new build: it sends READY to the
// watchdog unit and serves the release endpoints. Bump it when that contract changes.
const releaseProtocol = 1

// buildTime is stamped by the rollout build (-ldflags -X); hand builds report the commit time.
var buildTime string

// Build identifies the running build. Revision and Modified keep the shape `cos e2e` reads.
type Build struct {
	Protocol int    `json:"protocol"` // releaseProtocol: this build speaks READY and cos.<host>.release.*
	Host     string `json:"host"`
	Revision string `json:"revision"`
	Modified bool   `json:"modified"`
	Time     string `json:"time,omitempty"` // commit time
	Built    string `json:"built,omitempty"`
	Behavior string `json:"behavior,omitempty"`
	Good     string `json:"good,omitempty"`  // last-known-good revision on this host
	Trial    bool   `json:"trial,omitempty"` // this build is unconfirmed; the watchdog reverts it if it stops
}

func CurrentBuild(cfg Config) Build {
	v := Build{Protocol: releaseProtocol, Host: cfg.LocalHost, Built: buildTime}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				v.Revision = s.Value
			case "vcs.time":
				v.Time = s.Value
			case "vcs.modified":
				v.Modified = s.Value == "true"
			}
		}
	}
	if v.Built == "" {
		v.Built = v.Time
	}
	if b := CurrentBehavior(); b != nil {
		v.Behavior = b.Version // the main commit the hot-reloaded behavior files came from, or "embedded"
	}
	r := releasesOf(cfg)
	v.Good, _ = r.link("good")
	v.Trial = v.Revision != "" && r.trial() == v.Revision
	return v
}

func init() {
	cliCommands["version"] = cliCommand{Usage: "version", Run: func(_ context.Context, cfg Config, args []string, w io.Writer) error {
		if len(args) != 0 {
			return errors.New("usage: cos version")
		}
		return output(w, CurrentBuild(cfg))
	}}
}
