package scheduler

import "strings"

func quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func join(argv []any) string {
	parts := make([]string, 0, len(argv))
	for _, item := range argv {
		text, _ := item.(string)
		parts = append(parts, quote(text))
	}
	return strings.Join(parts, " ")
}

func stringMap(value any) map[string]string {
	out := map[string]string{}
	for key, item := range mapAny(value) {
		text, ok := item.(string)
		if ok {
			out[key] = text
		}
	}
	return out
}

func mapAny(value any) map[string]any {
	item, _ := value.(map[string]any)
	if item == nil {
		return map[string]any{}
	}
	return item
}
