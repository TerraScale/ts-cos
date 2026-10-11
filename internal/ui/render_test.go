package ui

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/templ"
)

// render writes a component to a string, with children (if any) in context.
func render(t *testing.T, children templ.Component, c templ.Component) string {
	t.Helper()
	ctx := context.Background()
	if children != nil {
		ctx = templ.WithChildren(ctx, children)
	}
	var b strings.Builder
	if err := c.Render(ctx, &b); err != nil {
		t.Fatalf("render: %v", err)
	}
	return b.String()
}

// renderCase is one component render check; every want substring must appear.
type renderCase struct {
	name     string
	children templ.Component
	c        templ.Component
	want     []string
}

func assertRenders(t *testing.T, tests []renderCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := render(t, tt.children, tt.c)
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("output missing %q:\n%s", want, got)
				}
			}
			if strings.Contains(got, "style=") {
				t.Errorf("output has inline style attribute:\n%s", got)
			}
		})
	}
}
