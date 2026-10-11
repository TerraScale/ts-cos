package cos

import (
	"fmt"
	"strings"
)

// threadModelGuard rejects a worker-only model, or a codex serviceTier other than default, anywhere
// in the model selection of a launch, dispatch, model or handoff request.
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
	pol := CurrentBehavior().Models
	if model, ok := workerOnlyModel(pol, value); ok {
		return OperationError(fmt.Sprintf("%s is a worker-only model; threads run on an advisor model: %s", model, strings.Join(pol.advisorModels(), ", ")))
	}
	if fastTier(value) {
		return OperationError("codex runs on the Standard tier (serviceTier default), never Fast")
	}
	return nil
}

// workerOnlyModel finds a worker-only model under any "model" key, at any depth.
func workerOnlyModel(pol ModelPolicy, v any) (string, bool) {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			if strings.EqualFold(k, "model") {
				if model, ok := anyWorkerOnly(pol, x); ok {
					return model, true
				}
			}
			if model, ok := workerOnlyModel(pol, x); ok {
				return model, true
			}
		}
	case []any:
		for _, x := range v {
			if model, ok := workerOnlyModel(pol, x); ok {
				return model, true
			}
		}
	}
	return "", false
}

// anyWorkerOnly reports a worker-only name in any string inside v, at any depth.
func anyWorkerOnly(pol ModelPolicy, v any) (string, bool) {
	switch v := v.(type) {
	case string:
		return v, pol.WorkerOnlyModel(v)
	case map[string]any:
		for _, x := range v {
			if model, ok := anyWorkerOnly(pol, x); ok {
				return model, true
			}
		}
	case []any:
		for _, x := range v {
			if model, ok := anyWorkerOnly(pol, x); ok {
				return model, true
			}
		}
	}
	return "", false
}

// fastTier reports a serviceTier other than default, as a key or as an option id, anywhere in v.
func fastTier(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		if tier, ok := v["serviceTier"]; ok && tier != "default" {
			return true
		}
		if v["id"] == "serviceTier" && v["value"] != "default" {
			return true
		}
		for _, x := range v {
			if fastTier(x) {
				return true
			}
		}
	case []any:
		for _, x := range v {
			if fastTier(x) {
				return true
			}
		}
	}
	return false
}
