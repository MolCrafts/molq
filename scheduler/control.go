package scheduler

import (
	"context"
	"path"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// ControlDirectory is reserved for files created by the scheduler adapter.
const ControlDirectory = ".molq-control"

// ControlManifest identifies the file ownership record created before submission.
const ControlManifest = "owned-v1"

// ControlFiles is the complete set of possible files for one managed allocation.
const ControlFiles = "stdout stderr job.sh launch exit cancel_requested"

// prepareControl creates a private allocation and its ownership record before
// dispatching a native mutation. Array logs have one control directory per member.
func (c *core) prepareControl(ctx context.Context, directory string, indices []int, files string) error {
	args := []string{"sh", "-c", `umask 077
root=$1; control=$2; marker=$3; files=$4; shift 4
mkdir -p -- "${root%/*}" && mkdir -- "$root" || exit 1
make_control() {
  mkdir -- "$1/$control" &&
  printf '%s\n' "$files" > "$1/$control/$marker"
}
if [ "$#" -eq 0 ]; then make_control "$root"; else
  for index do mkdir -- "$root/$index" && make_control "$root/$index" || exit 1; done
fi`, "sh", directory, ControlDirectory, ControlManifest, files}
	for _, index := range indices {
		args = append(args, itoa(index))
	}
	result, err := c.transport.Run(ctx, args, 30*time.Second, nil, false)
	if err != nil {
		return err
	}
	if result.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot create managed job directory", map[string]any{
			"path": directory, "stderr": string(result.Stderr[:min(len(result.Stderr), 4096)]),
		})
	}
	return nil
}

func controlPath(directory, name string) string {
	return path.Join(directory, ControlDirectory, name)
}
