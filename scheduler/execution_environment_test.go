package scheduler

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func identityLaunch(_ map[string]any, payload string) (string, error) { return payload, nil }

func TestGlobalEnvironmentRenderedOnceForManyUnits(t *testing.T) {
	large := strings.Repeat("x", 500*1024)
	units, children := make([]any, 256), make([]any, 256)
	for i := range units {
		id := itoa(i)
		units[i] = map[string]any{"id": id, "command": map[string]any{"kind": "argv", "argv": []any{"true"}}}
		children[i] = map[string]any{"kind": "unit", "ref": id}
	}
	spec := map[string]any{"execution": map[string]any{"env": map[string]any{"LARGE_GLOBAL": large}, "units": units, "plan": map[string]any{"kind": "parallel", "children": children}}}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 1<<20 {
		t.Fatal("fixture must fit a protocol frame")
	}
	output, err := RenderExecution(spec, identityLaunch)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(output, large) != 1 || strings.Count(output, "export LARGE_GLOBAL=") != 1 {
		t.Fatal("global environment repeated per unit")
	}
	if len(output) > len(encoded)+64*1024 {
		t.Fatalf("rendered output amplified: request %d bytes, script %d bytes", len(encoded), len(output))
	}
	t.Logf("256 units: request %d bytes, rendered script %d bytes", len(encoded), len(output))
}

func runExecutionScript(t *testing.T, script string) ([]byte, error) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("local Bash execution is POSIX-only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash")
	cmd.Stdin = strings.NewReader(script)
	cmd.WaitDelay = time.Second
	return cmd.CombinedOutput()
}

func TestGlobalAndUnitEnvironmentsPreserveCompositionAndIsolation(t *testing.T) {
	root := t.TempDir()
	other := filepath.Join(root, "other")
	if err := os.Mkdir(other, 0755); err != nil {
		t.Fatal(err)
	}
	shared := "quote ' double \" dollar $ slash \\ newline\nsecond line"
	override := "unit ' override"
	unit := func(id string, environment map[string]any) map[string]any {
		return map[string]any{"id": id, "env": environment, "command": map[string]any{"kind": "shell", "command": "export SHARED=changed; export UNIT_ONLY=child; exit 0"}}
	}
	first := unit("first", map[string]any{"OVERRIDE": override, "UNIT_ONLY": "first"})
	first["launch"] = map[string]any{"kind": "mpi", "threads": 3}
	second := unit("second", map[string]any{"PWD": "/virtual/unit", "OMP_NUM_THREADS": "unit"})
	second["cwd"] = other
	third := unit("third", nil)
	final := unit("final", nil)
	refs := func(ids ...string) []any {
		items := make([]any, len(ids))
		for i, id := range ids {
			items[i] = map[string]any{"kind": "unit", "ref": id}
		}
		return items
	}
	plan := map[string]any{"kind": "sequence", "children": []any{
		map[string]any{"kind": "unit", "ref": "first"},
		map[string]any{"kind": "parallel", "children": refs("second", "third")},
		map[string]any{"kind": "unit", "ref": "final"},
	}}
	spec := map[string]any{"execution": map[string]any{"cwd": root, "env": map[string]any{
		"SHARED": shared, "OVERRIDE": "global", "OMP_NUM_THREADS": "global", "PWD": "/virtual/global", "OLDPWD": "/virtual/old", "code": "global-code", "result": "global-result", "pids": "global-pids",
	}, "units": []any{first, second, third, final}, "plan": plan}}
	launch := func(unit map[string]any, payload string) (string, error) {
		id := unit["id"].(string)
		var checks []string
		for _, key := range []string{"SHARED", "OVERRIDE", "UNIT_ONLY", "OMP_NUM_THREADS", "PWD", "OLDPWD", "code", "result", "pids"} {
			checks = append(checks, "printf '%s' \"${"+key+"-}\" > "+quote(filepath.Join(root, id+"-"+key)))
		}
		checks = append(checks, payload)
		return strings.Join(checks, "\n"), nil
	}
	rendered, err := RenderExecution(spec, launch)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := runExecutionScript(t, rendered); err != nil {
		t.Fatalf("execution failed: %v: %s", err, output)
	}
	for _, id := range []string{"first", "second", "third", "final"} {
		expected := map[string]string{"SHARED": shared, "OVERRIDE": "global", "UNIT_ONLY": "", "OMP_NUM_THREADS": "global", "PWD": "/virtual/global", "OLDPWD": "/virtual/old", "code": "global-code", "result": "global-result", "pids": "global-pids"}
		if id == "first" {
			expected["OVERRIDE"] = override
			expected["UNIT_ONLY"] = "first"
			expected["OMP_NUM_THREADS"] = "3"
		}
		if id == "second" {
			expected["PWD"] = "/virtual/unit"
			expected["OMP_NUM_THREADS"] = "unit"
		}
		for key, want := range expected {
			got, err := os.ReadFile(filepath.Join(root, id+"-"+key))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Fatalf("unit %s env %s=%q, want %q", id, key, got, want)
			}
		}
	}
}

func TestSharedEnvironmentDoesNotChangePlanExitStatus(t *testing.T) {
	for _, kind := range []string{"sequence", "parallel"} {
		t.Run(kind, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "second")
			spec := map[string]any{"execution": map[string]any{"env": map[string]any{"result": "99", "code": "99", "pids": "99"}, "units": []any{
				map[string]any{"id": "first", "command": map[string]any{"kind": "shell", "command": "exit 7"}},
				map[string]any{"id": "second", "command": map[string]any{"kind": "shell", "command": "printf done > " + quote(marker)}},
			}, "plan": map[string]any{"kind": kind, "children": []any{map[string]any{"kind": "unit", "ref": "first"}, map[string]any{"kind": "unit", "ref": "second"}}}}}
			rendered, err := RenderExecution(spec, identityLaunch)
			if err != nil {
				t.Fatal(err)
			}
			output, err := runExecutionScript(t, rendered)
			failure, ok := err.(*exec.ExitError)
			if !ok || failure.ExitCode() != 7 {
				t.Fatalf("exit error %v, output %s", err, output)
			}
			_, err = os.Stat(marker)
			if kind == "sequence" && !os.IsNotExist(err) {
				t.Fatal("sequence continued after failure")
			}
			if kind == "parallel" && err != nil {
				t.Fatal("parallel failed to finish sibling")
			}
		})
	}
}

func TestOverriddenGlobalEnvironmentIsNeverApplied(t *testing.T) {
	spec := specWith(nil, nil, nil)
	execution := mapAny(spec["execution"])
	execution["env"] = map[string]any{"BASH_XTRACEFD": "invalid descriptor"}
	unit := mapAny(execution["units"].([]any)[0])
	unit["env"] = map[string]any{"BASH_XTRACEFD": "2"}
	unit["command"] = map[string]any{"kind": "argv", "argv": []any{"true"}}
	rendered, err := RenderExecution(spec, identityLaunch)
	if err != nil {
		t.Fatal(err)
	}
	output, err := runExecutionScript(t, rendered)
	if err != nil || len(output) != 0 {
		t.Fatalf("overridden global was applied: %v: %s", err, output)
	}
}
