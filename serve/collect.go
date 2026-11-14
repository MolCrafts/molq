package serve

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/MolCrafts/molq/scheduler"
)

// jobIdentity is the name submit gives each job directory under JobRoot.
var jobIdentity = regexp.MustCompile(`^[0-9a-f]{32}$`)

// archive is the serve-host directory that receives an SSH Cluster's job
// directories. Local targets already live on the serve host and have none.
func (s *Service) archive(cluster map[string]any) string {
	if clusterTransport(cluster)["kind"] != "ssh" {
		return ""
	}
	return filepath.Join(filepath.Dir(s.Registry.Path), "collected", stringField(cluster["id"]))
}

// collectable names the job directory submit created for a confirmed terminal
// job, on the target or already in archive. Jobs placed elsewhere, such as ones
// submitted outside molq, have none.
func collectable(job scheduler.Job, archive string) string {
	if !scheduler.Terminal(job.State) || job.Workdir == nil {
		return ""
	}
	dir := path.Clean(*job.Workdir)
	identity, parent := path.Base(dir), path.Dir(dir)
	onTarget := strings.HasSuffix(parent, "/"+scheduler.JobRoot)
	if !jobIdentity.MatchString(identity) || !(onTarget || (archive != "" && parent == filepath.ToSlash(archive))) {
		return ""
	}
	return identity
}

// collectTerminal collects every terminal job in one observation. Callers must
// not hold the Cluster's native slot.
func (s *Service) collectTerminal(ctx context.Context, cluster map[string]any, jobs []scheduler.Job) {
	for _, job := range jobs {
		s.rememberTerminalDirectory(ctx, cluster, job)
		s.collect(ctx, cluster, job)
	}
}

// collect moves a terminal job's directory from an SSH target to the serve
// host and returns the committed local copy, or "" when it is unavailable.
func (s *Service) collect(ctx context.Context, cluster map[string]any, job scheduler.Job) string {
	archive := s.archive(cluster)
	identity := collectable(job, archive)
	if identity == "" || archive == "" {
		return ""
	}
	final := filepath.Join(archive, identity)
	if isDirectory(final) && !collectionNeedsCleanup(final) {
		return final
	}
	root, err := s.jobRoot(ctx, cluster)
	if err != nil || !managedJobPath(root, path.Clean(*job.Workdir)) {
		return ""
	}
	s.mu.Lock()
	if s.collections == nil {
		s.collections = map[string]chan struct{}{}
	}
	if running := s.collections[final]; running != nil {
		s.mu.Unlock()
		select {
		case <-running:
		case <-ctx.Done():
		}
		if isDirectory(final) {
			return final
		}
		return ""
	}
	done := make(chan struct{})
	s.collections[final] = done
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.collections, final)
		s.mu.Unlock()
		close(done)
	}()
	if err := s.moveJobDirectory(ctx, cluster, path.Clean(*job.Workdir), final); err != nil {
		fmt.Fprintf(os.Stderr, "molq serve: job directory %s stays on Cluster %s: %s\n", identity, stringField(cluster["name"]), mustJSON(asData(err)))
		if !isDirectory(final) {
			return ""
		}
	}
	return final
}

// collectedPath maps a path inside the job's observed directory to the
// serve-host copy of that directory.
func collectedPath(local string, workdir *string, target string) (string, bool) {
	if local == "" || workdir == nil {
		return "", false
	}
	relative, ok := strings.CutPrefix(path.Clean(target), path.Clean(*workdir)+"/")
	if !ok {
		return "", false
	}
	return filepath.Join(local, filepath.FromSlash(relative)), true
}

func managedJobPath(root, dir string) bool {
	return path.Dir(dir) == root && jobIdentity.MatchString(path.Base(dir))
}

func isDirectory(name string) bool {
	info, err := os.Stat(name)
	return err == nil && info.IsDir()
}

// setCollectionState keeps the availability of a verified copy separate from
// the scheduler's compute state.
func (s *Service) setCollectionState(cluster, snapshot map[string]any, job scheduler.Job) {
	archive := s.archive(cluster)
	if archive == "" || snapshot["terminal"] != true {
		return
	}
	if identity := collectable(job, archive); identity != "" {
		snapshot["collection_state"] = "pending"
		final := filepath.Join(archive, identity)
		if _, err := readCollectionReceipt(final); err == nil && isDirectory(final) {
			snapshot["collection_state"] = "committed"
		}
	}
}
