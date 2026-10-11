package cos

import "strings"

// Threads run on Opus 5.5 (user 2026-10-11); Haiku 5.5 only runs as delegate_task
// subagents inside a thread, so no thread may be launched on or switched to Haiku.
const haikuThreadError = "threads run on Opus 5.5 (effort high, xhigh for high stakes); Haiku 5.5 is only for delegate_task subagents inside a thread"

// threadModelGuard rejects a Haiku model anywhere in the model selection of a
// launch, dispatch, model or handoff request.
func threadModelGuard(command string, args []string) error {
	arg := map[string]int{"launch": 0, "dispatch": 0, "model": 1, "handoff": 1}
	i, ok := arg[command]
	if !ok || len(args) <= i {
		return nil
	}
	value, err := objectJSON(args[i])
	if err != nil {
		return nil // the command's own validation reports bad JSON
	}
	if haikuModel(value) {
		return OperationError(haikuThreadError)
	}
	return nil
}

func haikuModel(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			if s, ok := x.(string); ok && k == "model" && strings.Contains(strings.ToLower(s), "haiku") {
				return true
			}
			if haikuModel(x) {
				return true
			}
		}
	case []any:
		for _, x := range v {
			if haikuModel(x) {
				return true
			}
		}
	}
	return false
}
