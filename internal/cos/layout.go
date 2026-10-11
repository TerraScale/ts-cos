package cos

import "time"

// localTime swaps a <time> element's UTC text for the browser's local time, short for today.
const localTime = `((d) => d.toDateString() === new Date().toDateString() ? d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'}) : d.toLocaleString([], {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'}))(new Date(el.dateTime))`

// clock is the server-side text for t: time of day when it is today (UTC), else month and day.
func clock(t time.Time) string {
	switch {
	case t.IsZero():
		return "—"
	case t.UTC().Format(time.DateOnly) == time.Now().UTC().Format(time.DateOnly):
		return t.UTC().Format("15:04") + " UTC"
	default:
		return t.UTC().Format("Jan 2 15:04") + " UTC"
	}
}

// parseStamp reads an RFC3339 stamp; an empty or bad one is the zero time.
func parseStamp(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}
