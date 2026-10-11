package ui

import "strings"

func displayName(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// passwordSignal names a local Datastar signal owned by one password field.
func passwordSignal(id, name string) string {
	return "_" + name + "_" + strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, id)
}

func passwordSignals(id string) string {
	return "{" + passwordSignal(id, "reveal") + ": false, " + passwordSignal(id, "filled") + ": false}"
}
