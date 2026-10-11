package cos

import (
	"context"
	"fmt"
	"math"
	"time"
)

// A miss can be a transcript that grew between the host's publish and its count, so the call is retried.
const (
	usageVerifyAttempts = 4 // one call and three retries
	usageVerifyRetry    = 10 * time.Second
	usageMirrorWait     = 15 * time.Second
	usageMirrorPoll     = 2 * time.Second
)

// checkTokenAccounting proves one host's token accounting (TS-826): its transcripts, its COS_USAGE rows
// and the runner's SQLite mirror of them agree within 2%.
func checkTokenAccounting(host string) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		var out usageVerifyReport
		var miss string
		for attempt := range usageVerifyAttempts {
			if attempt > 0 {
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-time.After(usageVerifyRetry):
				}
			}
			if out, err = callUsageVerify(ctx, bus, host, nil); err != nil {
				return "", err
			}
			if out.Transcript.Messages == 0 {
				return "", E2ESkip(fmt.Sprintf("no Claude usage today on %s", host))
			}
			if miss = usageMismatch(out); miss == "" {
				break
			}
		}
		if miss != "" {
			return "", fmt.Errorf("%s today: %s, after %d attempts", host, miss, usageVerifyAttempts)
		}
		evidence := fmt.Sprintf("%s today: transcripts %s tokens / %s out (%d msgs), COS_USAGE %s / %s (Δ %.1f%%)",
			host, usageHuman(out.Transcript.Tokens), usageHuman(out.Transcript.Output), out.Transcript.Messages,
			usageHuman(out.KV.Tokens), usageHuman(out.KV.Output), usageDelta(out.KV.Tokens, out.Transcript.Tokens))
		local := e.LocalHost()
		if host == local {
			return evidence, nil
		}
		delta, err := awaitMirror(ctx, bus, local, host, out.KV.Tokens)
		if err != nil {
			return "", err
		}
		return evidence + fmt.Sprintf(", mirror on %s Δ %.1f%%", local, delta), nil
	}
}

func callUsageVerify(ctx context.Context, bus *Bus, host string, args []string) (usageVerifyReport, error) {
	var r usageVerifyReport
	err := bus.Call(ctx, host, "usage-verify", args, &r)
	return r, err
}

// awaitMirror polls the runner's SQLite until host's rows match the bucket, and returns the miss in percent.
func awaitMirror(ctx context.Context, bus *Bus, local, host string, want int64) (float64, error) {
	deadline := time.Now().Add(usageMirrorWait)
	for {
		r, err := callUsageVerify(ctx, bus, local, []string{"mirror"})
		if err != nil {
			return 0, err
		}
		got := r.Mirror[host].Tokens
		if withinTwoPercent(got, want) {
			return usageDelta(got, want), nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("mirror on %s has %s tokens for %s, COS_USAGE has %s, want within 2%% after %s",
				local, usageHuman(got), host, usageHuman(want), usageMirrorWait)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(usageMirrorPoll):
		}
	}
}

// usageMismatch names the first total that misses the 2% bound, or returns "" when both match.
func usageMismatch(r usageVerifyReport) string {
	switch {
	case !withinTwoPercent(r.KV.Tokens, r.Transcript.Tokens):
		return fmt.Sprintf("tokens: COS_USAGE %s vs transcripts %s", usageHuman(r.KV.Tokens), usageHuman(r.Transcript.Tokens))
	case !withinTwoPercent(r.KV.Output, r.Transcript.Output):
		return fmt.Sprintf("output: COS_USAGE %s vs transcripts %s", usageHuman(r.KV.Output), usageHuman(r.Transcript.Output))
	}
	return ""
}

// usageDelta is how far got is from want, in percent.
func usageDelta(got, want int64) float64 {
	if want == 0 {
		if got == 0 {
			return 0
		}
		return 100
	}
	return 100 * math.Abs(float64(got-want)) / float64(want)
}

// usageHuman shortens a count for evidence: 12345678 is 12.3M and 410000 is 410k.
func usageHuman(n int64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	}
	return fmt.Sprint(n)
}
