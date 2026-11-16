package scheduler

import (
	"context"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

var slurmFailureReasons = map[string]string{
	"OUT_OF_MEMORY": "out_of_memory", "NODE_FAIL": "node_failure", "PREEMPTED": "preempted",
	"TIMEOUT": "timeout", "CANCELLED": "cancelled", "BOOT_FAIL": "boot_failure",
	"DEADLINE": "deadline", "FAILED": "failed",
}

func arrayIndices(spec map[string]any) ([]int, error) {
	array := mapAny(mapAny(spec["scheduling"])["array"])
	if len(array) == 0 {
		return nil, nil
	}
	var indices []int
	for _, value := range anySlice(array["indices"]) {
		index, ok := AsInt(value)
		if !ok || index < 0 {
			return nil, protocol.Fail("INVALID_INPUT", "Invalid array index", nil)
		}
		indices = append(indices, index)
	}
	if max, ok := AsInt(array["max_concurrent"]); ok && max > len(indices) {
		return nil, protocol.Fail("INVALID_INPUT", "Array concurrency exceeds its element count", nil)
	}
	sort.Ints(indices)
	return indices, nil
}

func arrayOption(spec map[string]any, indices []int) string {
	values := make([]string, len(indices))
	for i, index := range indices {
		values[i] = itoa(index)
	}
	value := strings.Join(values, ",")
	if max, ok := AsInt(mapAny(mapAny(spec["scheduling"])["array"])["max_concurrent"]); ok {
		value += "%" + itoa(max)
	}
	return value
}

func slurmIdentity(row map[string]any, source, identity string) (string, string, error) {
	parent, index := slurmNumber(row["array_job_id"]), slurmNumber(row["array_task_id"])
	if source == "accounting" {
		array := mapAny(row["array"])
		parent, index = slurmNumber(array["job_id"]), slurmNumber(array["task_id"])
	}
	if parent == nil || *parent == 0 {
		return identity, "", nil
	}
	if *parent < 0 || index == nil || *index < 0 {
		return "", "", protocol.Fail("SCHEDULER_UNAVAILABLE", "Slurm did not resolve array element identity", nil)
	}
	return itoa(*parent) + "_" + itoa(*index), itoa(*index), nil
}

func (s *Slurm) acceptedArray(ctx context.Context, parent string, indices []int) (Job, error) {
	ids := make([]string, len(indices))
	for i, index := range indices {
		ids[i] = parent + "_" + itoa(index)
	}
	for range 4 {
		jobs, err := s.QueryMany(ctx, ids, "")
		if err != nil {
			break
		}
		members := make([]Job, 0, len(ids))
		for _, id := range ids {
			if job := jobs[id]; job.Incarnation != "" {
				members = append(members, job)
			}
		}
		if len(members) == len(ids) {
			return Job{NativeID: parent, Members: members}, nil
		}
		select {
		case <-ctx.Done():
			return Job{}, submissionUnknown(parent)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return Job{}, submissionUnknown(parent)
}

func slurmParallel(plan map[string]any) bool {
	if plan["kind"] == "parallel" {
		return true
	}
	for _, child := range anySlice(plan["children"]) {
		if slurmParallel(mapAny(child)) {
			return true
		}
	}
	return false
}

func (s *Slurm) launchIsolated(unit map[string]any, payload string, memory int64) (string, error) {
	launch, placement := mapAny(unit["launch"]), mapAny(unit["placement"])
	ranks, cpus := 1, 1
	if launch["kind"] == "mpi" {
		ranks, _ = AsInt(launch["ranks"])
		if threads, ok := AsInt(launch["threads"]); ok {
			cpus = threads
		}
	} else if threads, ok := AsInt(placement["cpus_per_task"]); ok {
		cpus = threads
	}
	return "srun --exclusive --exact --nodes=1 --cpu-bind=threads --gres=none --ntasks=" + itoa(ranks) +
		" --cpus-per-task=" + itoa(cpus) + " --mem-per-cpu=" + strconv.FormatInt(memory, 10) + "M " + payload, nil
}

// Slurm 25.05 JSON can retain grouped pending tasks even with --array.
func expandSlurmArrayRow(row map[string]any, source string) ([]map[string]any, error) {
	group, key, field := row, "array_task_id", "array_task_string"
	if source == "accounting" {
		group, key, field = mapAny(row["array"]), "task_id", "task"
	}
	expression := stringField(group[field])
	if expression == "" || slurmNumber(group[key]) != nil {
		return []map[string]any{row}, nil
	}
	bad := protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid or oversized Slurm array index group", nil)
	expression, _, _ = strings.Cut(expression, "%")
	expression = strings.Trim(expression, "[]")
	var rows []map[string]any
	for _, span := range strings.Split(expression, ",") {
		rangeText, stepText, hasStep := strings.Cut(span, ":")
		step := uint64(1)
		if hasStep {
			value, err := strconv.ParseUint(stepText, 10, 31)
			if err != nil || value == 0 {
				return nil, bad
			}
			step = value
		}
		first, last, interval := strings.Cut(rangeText, "-")
		start, err := strconv.ParseUint(first, 10, 31)
		if err != nil {
			return nil, bad
		}
		end := start
		if interval {
			end, err = strconv.ParseUint(last, 10, 31)
			if err != nil || end < start {
				return nil, bad
			}
		}
		for index := start; index <= end; index += step {
			if len(rows) >= 1024 {
				return nil, bad
			}
			member := maps.Clone(row)
			if source == "accounting" {
				array := maps.Clone(group)
				array[key] = int(index)
				member["array"] = array
			} else {
				member[key] = int(index)
			}
			rows = append(rows, member)
		}
	}
	return rows, nil
}

func slurmStepMemory(resources map[string]any) (int64, error) {
	text, err := memoryMiB(resources)
	if err != nil {
		return 0, err
	}
	memory, _ := strconv.ParseInt(text, 10, 64)
	tasks, _ := AsInt(resources["tasks"])
	cpus := defaultInt(resources["cpus_per_task"], 1)
	if tasks <= 0 || cpus <= 0 || memory/int64(tasks)/int64(cpus) < 1 {
		return 0, unsupported("Parallel Slurm steps require explicit memory of at least 1 MiB per allocated CPU")
	}
	return memory / int64(tasks) / int64(cpus), nil
}

// slurmDiagnostic retains native reason and exit details without treating a
// pending reason or transition flag as a terminal failure.
func slurmDiagnostic(row map[string]any, source string) map[string]any {
	out := map[string]any{}
	if source == "queue" {
		if reason := row["state_reason"]; nonempty(reason) {
			out["reason"] = reason
		}
	} else if reason := mapAny(row["state"])["reason"]; nonempty(reason) {
		out["reason"] = reason
	}
	for _, key := range []string{"state_description", "exit_code", "derived_exit_code", "failed_node", "system_comment", "admin_comment", "restart_cnt"} {
		if value := row[key]; nonempty(value) {
			out[key] = value
		}
	}
	var failures []map[string]any
	for _, value := range anySlice(row["steps"]) {
		step := mapAny(value)
		state := firstString(step["state"])
		if slurmFailureReasons[state] == "" {
			continue
		}
		failures = append(failures, map[string]any{"id": mapAny(step["step"])["id"], "state": step["state"], "exit_code": step["exit_code"]})
	}
	if len(failures) != 0 {
		out["failed_steps"] = failures
	}
	return out
}

func slurmFailureReason(token string, row map[string]any) string {
	if token == "FAILED" {
		for _, value := range anySlice(row["steps"]) {
			step := mapAny(value)
			if firstString(step["state"]) == "OUT_OF_MEMORY" {
				return "out_of_memory"
			}
		}
	}
	return slurmFailureReasons[token]
}
