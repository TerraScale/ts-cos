package cos

import (
	"testing"
	"time"
)

func TestReplyIDIsStablePerRun(t *testing.T) {
	at := time.Date(2026, 10, 11, 1, 2, 3, 4, time.UTC)
	a, b := replyID("run:thread:x:ordinal:1", at), replyID("run:thread:x:ordinal:1", at)
	if a != b || replyID("run:thread:x:ordinal:2", at) == a {
		t.Fatal("reply IDs must be stable per run and differ between runs:", a, b)
	}
	if replyID("run:thread:x:ordinal:1", at.Add(time.Second)) <= a {
		t.Fatal("reply IDs must sort by request time")
	}
	if replyID("r", time.Time{}) == "" {
		t.Fatal("zero request time needs a fallback ID")
	}
}
