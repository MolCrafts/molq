package scheduler

import (
	"context"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// prepareRun creates one private run directory inside an existing project.
// Its reserved ancestors cannot be symlinks. Array members have separate logs.
func (c *core) prepareRun(ctx context.Context, directory string, indices []int) error {
	args := []string{"sh", "-c", `umask 077
root=$1; shift
jobs=${root%/*}; state=${jobs%/*}; project=${state%/*}
[ -d "$project" ] && [ ! -L "$project" ] || exit 1
mkdir -p -- "$state" && [ ! -L "$state" ] || exit 1
mkdir -p -- "$jobs" && [ ! -L "$jobs" ] || exit 1
mkdir -- "$root" || exit 1
for index do mkdir -- "$root/$index" || exit 1; done`, "sh", directory}
	for _, index := range indices {
		args = append(args, itoa(index))
	}
	result, err := c.transport.Run(ctx, args, 30*time.Second, nil, false)
	if err != nil {
		return err
	}
	if result.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot create project job directory", map[string]any{
			"path": directory, "stderr": string(result.Stderr[:min(len(result.Stderr), 4096)]),
		})
	}
	return nil
}

var runIdentity = regexp.MustCompile(`^[0-9a-f]{32}$`)

// RunLocation returns the project and run ID of an allocation directory
// (or of a managed array element's directory).
func RunLocation(directory string) (project, runID string) {
	if !path.IsAbs(directory) || path.Clean(directory) != directory || strings.ContainsAny(directory, "\r\n\x00") {
		return "", ""
	}
	if index, err := strconv.ParseUint(path.Base(directory), 10, 31); !runIdentity.MatchString(path.Base(directory)) && err == nil && strconv.FormatUint(index, 10) == path.Base(directory) {
		directory = path.Dir(directory)
	}
	parent := path.Dir(directory)
	runID = path.Base(directory)
	if !strings.HasSuffix(parent, "/"+ProjectJobs) || !runIdentity.MatchString(runID) {
		return "", ""
	}
	project = strings.TrimSuffix(parent, "/"+ProjectJobs)
	return project, runID
}
