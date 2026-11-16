package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

// Scheduler is one native scheduler implementation.
type Scheduler interface {
	Validate(spec map[string]any) error
	Render(spec map[string]any, directory string) (string, error)
	Submit(ctx context.Context, spec map[string]any, directory string) (Job, error)
	RecoverSubmission(ctx context.Context, lookup SubmissionLookup) (Job, error)
	QueryMany(ctx context.Context, ids []string, projectRoot string) (map[string]Job, error)
	List(ctx context.Context, projectRoot string) ([]Job, error)
	History(ctx context.Context, ids []string) (map[string]Job, error)
	HistoryRange(ctx context.Context, since, until time.Time) ([]Job, error)
	Requeue(ctx context.Context, job Job) error
	Cancel(ctx context.Context, job Job) (map[string]any, error)
}

// ProjectJobs is the project-relative directory for MolQ runs.
const ProjectJobs = ".molq/jobs"

// Create selects the destination's scheduler. Unknown names are unsupported.
func Create(cluster map[string]any, target transport.Transport) (Scheduler, error) {
	switch cluster["scheduler"] {
	case "shell":
		return NewShell(target, cluster), nil
	case "slurm":
		return NewSlurm(target, cluster), nil
	case "pbs":
		return NewPBS(target, cluster), nil
	case "lsf":
		return NewLSF(target, cluster), nil
	default:
		name := fmt.Sprint(cluster["scheduler"])
		return nil, protocol.Fail("SCHEDULER_UNSUPPORTED", "Unknown scheduler "+name, nil)
	}
}
