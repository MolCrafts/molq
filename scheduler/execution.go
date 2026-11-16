package scheduler

import (
	"sort"
	"strings"
)

// RenderExecution turns a validated plan into the allocation-internal script.
func RenderExecution(spec map[string]any, launch func(map[string]any, string) (string, error)) (string, error) {
	execution := mapAny(spec["execution"])
	units, _ := execution["units"].([]any)
	var lines []string
	globalEnvironment := stringMap(execution["env"])
	if len(globalEnvironment) > 0 {
		// Keep large allocation-wide values in one function. Invoke it after
		// each unit's cd so variables such as PWD retain their original timing.
		lines = append(lines, "molq_environment() {")
		for _, key := range environmentKeys(globalEnvironment) {
			// An overridden global value must never be applied temporarily.
			lines = append(lines, `case "${1-}" in *"|`+key+`|"*) ;; *) export `+key+"="+quote(globalEnvironment[key])+" ;; esac")
		}
		lines = append(lines, "}")
	}
	names := map[string]string{}
	for index, item := range units {
		unit := mapAny(item)
		name := "molq_unit_" + itoa(index)
		id, _ := unit["id"].(string)
		names[id] = name
		command := mapAny(unit["command"])
		var payload string
		switch command["kind"] {
		case "argv":
			payload = join(command["argv"].([]any))
		case "shell":
			text, _ := command["command"].(string)
			payload = "bash -c " + quote(text)
		default:
			text, _ := command["text"].(string)
			payload = "bash -c " + quote(text)
		}
		body := []string{name + "() ("}
		cwd, _ := unit["cwd"].(string)
		if cwd == "" {
			cwd, _ = execution["cwd"].(string)
		}
		if cwd != "" {
			body = append(body, "cd -- "+quote(cwd)+" || exit 126")
		}
		environment := stringMap(unit["env"])
		keys := environmentKeys(environment)
		if len(globalEnvironment) > 0 {
			body = append(body, "molq_environment "+quote("|"+strings.Join(keys, "|")+"|"))
		}
		for _, key := range keys {
			body = append(body, "export "+key+"="+quote(environment[key]))
		}
		if threads, ok := AsInt(mapAny(unit["launch"])["threads"]); ok {
			body = append(body, "export OMP_NUM_THREADS="+itoa(threads))
		}
		launched, err := launch(unit, payload)
		if err != nil {
			return "", err
		}
		for _, pair := range []struct{ stream, flag string }{{"stdout", ">"}, {"stderr", "2>"}} {
			if path, ok := unit[pair.stream].(string); ok && path != "" {
				launched += " " + pair.flag + " " + quote(path)
			}
		}
		body = append(body, launched, ")")
		lines = append(lines, body...)
	}
	counter := 0
	var emit func(map[string]any) (string, error)
	emit = func(plan map[string]any) (string, error) {
		if plan["kind"] == "unit" {
			ref, _ := plan["ref"].(string)
			return names[ref], nil
		}
		children, _ := plan["children"].([]any)
		names := make([]string, 0, len(children))
		for _, child := range children {
			name, err := emit(mapAny(child))
			if err != nil {
				return "", err
			}
			names = append(names, name)
		}
		name := "molq_plan_" + itoa(counter)
		counter++
		lines = append(lines, name+"() (")
		if plan["kind"] == "sequence" {
			for _, child := range names {
				lines = append(lines, child+` || exit "$?"`)
			}
		} else {
			lines = append(lines, "pids=(); result=0")
			for _, child := range names {
				lines = append(lines, child+" &", `pids+=("$!")`)
			}
			lines = append(lines,
				`for pid in "${pids[@]}"; do`,
				`  code=0; wait "$pid" || code=$?`,
				`  if [ "$result" -eq 0 ] && [ "$code" -ne 0 ]; then result=$code; fi`,
				"done",
				`exit "$result"`,
			)
		}
		lines = append(lines, ")")
		return name, nil
	}
	root, err := emit(mapAny(execution["plan"]))
	if err != nil {
		return "", err
	}
	lines = append(lines, root, `exit "$?"`)
	return strings.Join(lines, "\n") + "\n", nil
}

func environmentKeys(environment map[string]string) []string {
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	negative := value < 0
	if negative {
		value = -value
	}
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
