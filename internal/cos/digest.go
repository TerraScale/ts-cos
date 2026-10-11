package cos

import (
	"cmp"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	digestMaxBytes = 4 << 10 // fixed hard cap for a COS wake digest
	digestLineMax  = 240     // runes per line, ellipsis included
	digestNamed    = 3       // entities named in a folded line before "+N"
)

// Kinds the COS agent must see first; kinds not listed rank after them.
var (
	digestOrder      = []EventKind{KindNeedsUser, KindRequestPending, KindTurnFailed, KindCIStatus, KindPRConflict, KindReview}
	digestSuperseded = []EventKind{KindCIStatus, KindPRSynchronized, KindPRMergeable, KindPRConflict}
	digestDigits     = regexp.MustCompile(`\d+`)
)

// DigestLine is one coalesced group of events in a WakeDigest.
type DigestLine struct {
	Kind        EventKind
	Severity    Severity
	Subject     string
	Summary     string
	Count       int
	First, Last time.Time
}

// WakeDigest is the changes-since-last-wake text for the COS agent. It is not named
// Digest because behavior.go already declares that type.
type WakeDigest struct {
	Lines   []DigestLine // lines in Text, most urgent first
	Events  int          // input events after the since filter
	Merged  int          // events with a repeated ID or superseded by a merged or closed PR
	Dropped int          // events cut by the size cap
	Text    string
	Bytes   int
}

// digestEntry is a DigestLine under construction, with what folding needs.
type digestEntry struct {
	DigestLine
	repo     string
	rank     int
	subjects []string
}

// BuildDigest folds the events after since into prioritized text of at most maxBytes.
// Counts: Events == Merged + sum of Count over Lines + Dropped.
func BuildDigest(events []Event, since time.Time, maxBytes int) WakeDigest {
	if maxBytes <= 0 {
		maxBytes = digestMaxBytes
	}
	var d WakeDigest
	seen := map[string]bool{}
	var fresh []Event
	for _, e := range events {
		if !e.Time.After(since) {
			continue
		}
		d.Events++
		if seen[e.ID] {
			d.Merged++
			continue
		}
		seen[e.ID] = true
		fresh = append(fresh, e)
	}
	// Time order makes each group's summary and rank come from its latest event.
	slices.SortStableFunc(fresh, func(a, b Event) int { return a.Time.Compare(b.Time) })

	closed := map[string]bool{}
	for _, e := range fresh {
		if (e.Kind == KindPRMerged || e.Kind == KindPRClosed) && e.PR != 0 {
			closed[prKey(e.Repo, e.PR)] = true
		}
	}
	groups := map[string]*digestEntry{}
	var order []*digestEntry
	for _, e := range fresh {
		// A merged or closed PR has no CI, sync or conflict state left to report.
		if e.PR != 0 && slices.Contains(digestSuperseded, e.Kind) && closed[prKey(e.Repo, e.PR)] {
			d.Merged++
			continue
		}
		key := strings.Join([]string{string(e.Kind), e.Repo, strconv.Itoa(e.PR), e.Ticket, e.Thread, e.Host, e.Item}, "\x00")
		g := groups[key]
		if g == nil {
			subject := digestSubject(e)
			g = &digestEntry{DigestLine: DigestLine{Kind: e.Kind, Severity: SeverityInfo, First: e.Time}, repo: e.Repo, subjects: []string{subject}}
			groups[key] = g
			order = append(order, g)
		}
		g.Count++
		g.Summary = e.Summary
		g.Last = e.Time
		g.Severity = maxSeverity(g.Severity, e.Severity)
		g.rank = digestRank(e)
	}

	var lines []*digestEntry
	similar := map[string]*digestEntry{}
	for _, g := range order {
		k := string(g.Kind) + "\x00" + g.repo + "\x00" + normSummary(g.Summary)
		if f := similar[k]; f != nil {
			f.absorb(g)
			continue
		}
		similar[k] = g
		lines = append(lines, g)
	}

	slices.SortStableFunc(lines, func(a, b *digestEntry) int {
		return cmp.Or(
			cmp.Compare(sevRank(b.Severity), sevRank(a.Severity)),
			cmp.Compare(a.rank, b.rank),
			b.Last.Compare(a.Last),
		)
	})
	all := make([]DigestLine, len(lines))
	for i, g := range lines {
		g.Subject = digestSubjectList(g.subjects)
		all[i] = g.DigestLine
	}
	text, shown, dropped := renderDigest(all, maxBytes)
	d.Text, d.Bytes, d.Dropped = text, len(text), dropped
	d.Lines = all[:shown]
	return d
}

// absorb folds a similar group into e: counts add, the latest summary wins, and the strongest severity and rank stay.
func (e *digestEntry) absorb(g *digestEntry) {
	if g.Last.After(e.Last) {
		e.Last, e.Summary = g.Last, g.Summary
	}
	if g.First.Before(e.First) {
		e.First = g.First
	}
	e.Count += g.Count
	e.Severity = maxSeverity(e.Severity, g.Severity)
	e.rank = min(e.rank, g.rank)
	e.subjects = append(e.subjects, g.subjects...)
}

// renderDigest writes lines in order until the next one, plus a footer for the rest, would exceed maxBytes.
func renderDigest(lines []DigestLine, maxBytes int) (text string, shown, dropped int) {
	left := 0
	for _, l := range lines {
		left += l.Count
	}
	var b strings.Builder
	for _, l := range lines {
		s := digestText(l) + "\n"
		// Reserve the footer for everything still unshown, so it always fits.
		if b.Len()+len(s)+len(digestFooter(left)) > maxBytes {
			dropped = left
			break
		}
		b.WriteString(s)
		shown++
		left -= l.Count
	}
	// A cap smaller than the footer gets no footer; Dropped still reports the cut.
	if f := digestFooter(dropped); dropped > 0 && b.Len()+len(f) <= maxBytes {
		b.WriteString(f)
	}
	return strings.TrimSuffix(b.String(), "\n"), shown, dropped
}

func digestFooter(n int) string {
	return fmt.Sprintf("+%d more events omitted (cos events)\n", n)
}

func digestText(l DigestLine) string {
	s := "[" + string(l.Severity) + "] " + string(l.Kind)
	if l.Subject != "" {
		s += " " + l.Subject
	}
	s += ": " + l.Summary
	if l.Count > 1 {
		s += fmt.Sprintf(" (x%d)", l.Count)
	}
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > digestLineMax {
		s = string(r[:digestLineMax-3]) + "..."
	}
	return s
}

// digestSubject names the entity: owner/repo#PR, then ticket, host/thread, or Inbox item.
func digestSubject(e Event) string {
	switch {
	case e.Repo != "" && e.PR != 0:
		return e.Repo + "#" + strconv.Itoa(e.PR)
	case e.Ticket != "":
		return e.Ticket
	case e.Host != "" && e.Thread != "":
		return e.Host + "/" + e.Thread
	case e.Thread != "":
		return e.Thread
	case e.Host != "":
		return e.Host
	}
	return e.Item
}

// digestSubjectList names up to digestNamed distinct entities, then "+N" for the rest.
func digestSubjectList(subjects []string) string {
	var uniq []string
	for _, s := range subjects {
		if s != "" && !slices.Contains(uniq, s) {
			uniq = append(uniq, s)
		}
	}
	if len(uniq) <= digestNamed {
		return strings.Join(uniq, ", ")
	}
	return strings.Join(uniq[:digestNamed], ", ") + fmt.Sprintf(" +%d", len(uniq)-digestNamed)
}

// digestRank orders events by kind; a failing CI run ranks with failures, a passing one after the list.
func digestRank(e Event) int {
	i := slices.Index(digestOrder, e.Kind)
	if i < 0 || (e.Kind == KindCIStatus && !ciFailing(e.Data)) {
		return len(digestOrder)
	}
	return i
}

func ciFailing(data json.RawMessage) bool {
	var d struct {
		State string `json:"state"`
	}
	return json.Unmarshal(data, &d) == nil && (d.State == "failure" || d.State == "error")
}

// normSummary keeps the shape of a summary so runs and counts with different numbers fold together.
func normSummary(s string) string {
	return digestDigits.ReplaceAllString(strings.Join(strings.Fields(strings.ToLower(s)), " "), "#")
}

func sevRank(s Severity) int {
	switch s {
	case SeverityUrgent:
		return 2
	case SeverityWarn:
		return 1
	}
	return 0
}

func maxSeverity(a, b Severity) Severity {
	if sevRank(b) > sevRank(a) {
		return b
	}
	return a
}
