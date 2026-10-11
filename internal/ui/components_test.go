package ui

import (
	"testing"

	"github.com/a-h/templ"
)

func TestComponentsRenderKitClasses(t *testing.T) {
	assertRenders(t, []renderCase{
		{
			name:     "card",
			children: templ.Raw("<p>BODY</p>"),
			c:        Card(CardProps{ID: "x", Title: "T"}),
			want:     []string{`class="ts-card"`, "ts-card-title", `id="x-title"`, "<p>BODY</p>"},
		},
		{
			name: "stats",
			children: templ.Join(
				Stat("Databases", "3", "in this project"),
				Stat("Users", "12", ""),
			),
			c:    Stats("Today"),
			want: []string{"ts-stats", "ts-stat-value", ">3<", ">12<", `aria-label="Today"`},
		},
		{
			name: "alert danger",
			c:    Alert("danger", "t", "m"),
			want: []string{"ts-alert-danger", `role="alert"`, "ts-alert-title", "ts-alert-body"},
		},
		{
			name: "badge",
			c:    Badge("success", "ok"),
			want: []string{"ts-badge-success", ">ok<"},
		},
		{
			name: "empty",
			c:    Empty("Nothing", "yet"),
			want: []string{"ts-empty", "ts-empty-title", "ts-empty-detail", ">yet<"},
		},
	})
}
