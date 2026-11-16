package serve

import (
	"encoding/json"

	"github.com/MolCrafts/molq/protocol"
)

// Normalize merges destination defaults and checks one allocation plan.
func Normalize(spec, defaults map[string]any) (map[string]any, error) {
	result := mapValue(deepCopy(spec))
	for _, section := range []string{"resources", "scheduling"} {
		merged := cloneMap(mapValue(defaults[section]))
		for key, value := range mapValue(result[section]) {
			merged[key] = value
		}
		result[section] = merged
	}
	execution := mapValue(result["execution"])
	result["execution"] = execution
	env := cloneMap(mapValue(defaults["env"]))
	for key, value := range mapValue(execution["env"]) {
		env[key] = value
	}
	execution["env"] = env
	if _, ok := execution["cwd"]; !ok {
		if cwd, ok := defaults["cwd"]; ok {
			execution["cwd"] = cwd
		}
	}
	if err := protocol.Current().ValidateDocument(result, "#/$defs/JobSpec"); err != nil {
		return nil, err
	}
	units, _ := execution["units"].([]any)
	ids := make([]string, 0, len(units))
	byID := map[string]map[string]any{}
	for _, item := range units {
		unit := mapValue(item)
		command := mapValue(unit["command"])
		if command["kind"] == "argv" {
			argv, _ := command["argv"].([]any)
			if len(argv) == 0 || argv[0] == "" {
				return nil, protocol.Fail("INVALID_INPUT", "argv program must not be empty", nil)
			}
		}
		id, _ := unit["id"].(string)
		ids = append(ids, id)
		if _, exists := byID[id]; exists {
			return nil, protocol.Fail("INVALID_INPUT", "Execution unit IDs must be unique", nil)
		}
		byID[id] = unit
	}
	plan, ok := execution["plan"].(map[string]any)
	if !ok {
		if len(units) != 1 {
			return nil, protocol.Fail("INVALID_INPUT", "Multiple units require an explicit execution plan", nil)
		}
		plan = map[string]any{"kind": "unit", "ref": ids[0]}
		execution["plan"] = plan
	}
	var seen []string
	tasks, cpus, err := visitPlan(plan, byID, &seen, 0)
	if err != nil {
		return nil, err
	}
	if !sameIDs(seen, ids) {
		return nil, protocol.Fail("INVALID_INPUT", "Every unit must appear exactly once in the plan", nil)
	}
	resources := mapValue(result["resources"])
	if budget, ok := asInt(resources["tasks"]); ok {
		perTask, hasPer := asInt(resources["cpus_per_task"])
		if !hasPer {
			perTask = 1
		}
		if tasks > budget || cpus > budget*perTask {
			return nil, protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "Execution exceeds allocation CPU/task budget", nil)
		}
	}
	accelerators, _ := resources["accelerators"].([]any)
	for _, item := range accelerators {
		acc := mapValue(item)
		if acc["kind"] == "nvidia_mps" {
			if _, ok := acc["model"]; ok {
				return nil, protocol.Fail("RESOURCE_REQUEST_INVALID", "MPS resource request cannot specify a GPU model", nil)
			}
		}
	}
	return result, nil
}

func visitPlan(plan map[string]any, byID map[string]map[string]any, seen *[]string, depth int) (int, int, error) {
	if depth > 16 {
		return 0, 0, protocol.Fail("INVALID_INPUT", "Execution plan depth exceeds 16", nil)
	}
	if plan["kind"] == "unit" {
		identity, _ := plan["ref"].(string)
		unit, ok := byID[identity]
		if !ok {
			return 0, 0, protocol.Fail("INVALID_INPUT", "Unknown unit reference", map[string]any{"unit": identity})
		}
		*seen = append(*seen, identity)
		launch := mapValue(unit["launch"])
		if len(launch) == 0 {
			launch = map[string]any{"kind": "direct"}
		}
		if launch["kind"] == "mpi" {
			if _, ok := launch["ranks"]; !ok {
				return 0, 0, protocol.Fail("INVALID_INPUT", "MPI requires explicit ranks", nil)
			}
		}
		if launch["kind"] == "direct" {
			if _, ok := launch["ranks"]; ok {
				return 0, 0, protocol.Fail("INVALID_INPUT", "Direct launch cannot contain MPI layout", nil)
			}
			if _, ok := launch["threads"]; ok {
				return 0, 0, protocol.Fail("INVALID_INPUT", "Direct launch cannot contain MPI layout", nil)
			}
		}
		placement := mapValue(unit["placement"])
		tasks := 1
		if value, ok := asInt(placement["tasks"]); ok {
			tasks = value
		}
		if value, ok := asInt(launch["ranks"]); ok {
			tasks = value
		}
		cpusPer := 1
		if value, ok := asInt(placement["cpus_per_task"]); ok {
			cpusPer = value
		}
		if value, ok := asInt(launch["threads"]); ok {
			cpusPer = value
		}
		if launch["kind"] == "mpi" {
			if value, ok := asInt(placement["tasks"]); ok && value != tasks {
				return 0, 0, protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "MPI ranks and unit placement disagree", nil)
			}
			if value, ok := asInt(placement["cpus_per_task"]); ok && value != cpusPer {
				return 0, 0, protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "MPI threads and unit placement disagree", nil)
			}
		}
		return tasks, tasks * cpusPer, nil
	}
	children, _ := plan["children"].([]any)
	tasks, cpus := 0, 0
	for i, child := range children {
		childTasks, childCPUs, err := visitPlan(mapValue(child), byID, seen, depth+1)
		if err != nil {
			return 0, 0, err
		}
		if i == 0 || plan["kind"] == "parallel" {
			if plan["kind"] == "parallel" {
				tasks += childTasks
				cpus += childCPUs
				continue
			}
			tasks, cpus = childTasks, childCPUs
			continue
		}
		if childTasks > tasks {
			tasks = childTasks
		}
		if childCPUs > cpus {
			cpus = childCPUs
		}
	}
	return tasks, cpus, nil
}

func sameIDs(seen, ids []string) bool {
	if len(seen) != len(ids) {
		return false
	}
	left := map[string]int{}
	right := map[string]int{}
	for _, id := range seen {
		left[id]++
	}
	for _, id := range ids {
		right[id]++
	}
	if len(left) != len(right) {
		return false
	}
	for key, count := range left {
		if right[key] != count {
			return false
		}
	}
	return true
}

func mapValue(value any) map[string]any {
	item, _ := value.(map[string]any)
	if item == nil {
		return map[string]any{}
	}
	return item
}

func deepCopy(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if json.Unmarshal(encoded, &out) != nil {
		return value
	}
	return out
}

func asInt(value any) (int, bool) {
	switch item := value.(type) {
	case int:
		return item, true
	case int64:
		return int(item), true
	case float64:
		return int(item), true
	default:
		return 0, false
	}
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}
