package cos

import (
	"strings"
	"testing"
)

func TestPresetButtons(t *testing.T) {
	body := `<div class="segmented">` +
		`<button type="button" aria-pressed="false" data-on:click="@post(&#39;/autopilot/interval/10&#39;)">10 min</button>` +
		`<button type="button" aria-pressed="false" data-on:click="@post(&#39;/autopilot/interval/15&#39;)">15 min</button>` +
		`<button type="button" aria-pressed="true" data-on:click="@post(&#39;/autopilot/interval/30&#39;)">30 min</button>` +
		`<button type="button" aria-pressed="false" data-on:click="@post(&#39;/autopilot/interval/60&#39;)">60 min</button>` +
		`</div>`
	got := presetButtons(body)
	for _, m := range autopilotPresets {
		if _, ok := got[m]; !ok {
			t.Fatalf("preset %d not rendered: %v", m, got)
		}
	}
	if !got[30] || got[10] || len(got) != 4 {
		t.Fatalf("presetButtons = %v, want 30 pressed and the other three not", got)
	}
	if missing := presetButtons(strings.Replace(body, "60 min</button>", "", 1)); len(missing) != 3 {
		t.Fatalf("presetButtons with 60 removed = %v, want three presets", missing)
	}
}

func TestIntervalSetting(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		found   bool
		want    int
		wantErr bool
	}{
		{"no stored value is the default", "", false, autopilotDefault, false},
		{"stored preset", "15", true, 15, false},
		{"stored value is not a preset", "45", true, 0, true},
		{"stored value is not a number", "soon", true, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := intervalSetting([]byte(tc.value), tc.found)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("intervalSetting = %d, %v; want %d, error %t", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
