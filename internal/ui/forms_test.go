package ui

import "testing"

func TestFormsRenderKitClasses(t *testing.T) {
	field := FieldProps{ID: "pw", Label: "Password", Hint: "At least 12 characters", Error: "Too short"}
	assertRenders(t, []renderCase{
		{
			name: "password input",
			c:    PasswordInput(PasswordProps{ID: "pw"}),
			want: []string{"ts-input", "ts-input-group", `type="password"`, `id="pw"`},
		},
		{
			name:     "field with hint and error",
			children: PasswordInput(PasswordProps{ID: "pw", Attrs: field.ControlAttrs()}),
			c:        Field(field),
			want:     []string{`for="pw"`, `id="pw-hint"`, `id="pw-error"`, `aria-describedby="pw-hint pw-error"`, `aria-invalid="true"`, "autofocus", ">Password<"},
		},
	})
}
