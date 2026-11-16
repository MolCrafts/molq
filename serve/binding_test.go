package serve

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentClusterRevisionKeepsSchedulerBinding(t *testing.T) {
	service, err := Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	definition := map[string]any{"name": "hpc", "scheduler": "slurm", "launcher": "srun"}
	if _, err := service.Call("clusters.register", definition); err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{"resources": map[string]any{"tasks": 2, "cpus_per_task": 1}, "execution": map[string]any{"units": []any{map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"true"}}, "launch": map[string]any{"kind": "mpi", "ranks": 2, "threads": 1}}}, "plan": map[string]any{"kind": "unit", "ref": "main"}}}
	errors := make(chan error, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				cluster, item, err := service.cluster("hpc")
				if err != nil {
					errors <- err
					return
				}
				script, err := item.Render(spec, "/scratch/job")
				if err != nil {
					errors <- err
					return
				}
				expected := "srun --exclusive"
				if cluster["launcher"] == "mpirun" {
					expected = "mpirun -np 2"
				}
				if !strings.Contains(script, expected) {
					errors <- fmt.Errorf("revision %v launcher %v bound to wrong Scheduler: %s", cluster["revision"], cluster["launcher"], script)
					return
				}
			}
		}()
	}
	for revision := 1; revision <= 100; revision++ {
		launcher := "mpirun"
		if revision%2 == 0 {
			launcher = "srun"
		}
		update := map[string]any{"name": "hpc", "scheduler": "slurm", "launcher": launcher}
		if _, err := service.Call("clusters.update", map[string]any{"cluster": "hpc", "expected_revision": revision, "definition": update}); err != nil {
			t.Fatal(err)
		}
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
