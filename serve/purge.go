package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

const (
	// defaultRetentionDays applies when settings.job_retention_days is absent.
	defaultRetentionDays = 30
	// PurgeInterval is how often a listening serve process purges.
	PurgeInterval = 24 * time.Hour
	purgeDeadline = 10 * time.Minute
)

// RetainJobDirectories purges once now and then every interval until ctx ends.
// Failures are reported on stderr and never stop serving.
func (s *Service) RetainJobDirectories(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.purgeAndReport(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) purgeAndReport(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, purgeDeadline)
	defer cancel()
	report, err := s.purge(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "molq serve: job directory purge failed: %s\n", mustJSON(asData(err)))
		return
	}
	removed := 0
	for _, result := range report["results"].([]map[string]any) {
		removed += len(result["removed"].([]string))
		if failure := result["error"]; failure != nil {
			fmt.Fprintf(os.Stderr, "molq serve: job directory purge skipped %s of Cluster %s: %s\n", result["location"], result["cluster_id"], mustJSON(failure))
		}
	}
	if removed > 0 {
		fmt.Fprintf(os.Stderr, "molq serve: deleted %d job directories older than %d days\n", removed, report["retention_days"])
	}
}

// retentionDays is the configured window; zero disables purging.
func (s *Service) retentionDays() int {
	if s.config == nil {
		return defaultRetentionDays
	}
	return s.config.RetentionDays(defaultRetentionDays)
}

// purge deletes every job directory older than the retention window on each
// Cluster target and in the serve-host archive, or only for one Cluster when
// name is set. Each target and archive is one result; one failure never stops
// the others.
func (s *Service) purge(ctx context.Context, name string) (map[string]any, error) {
	s.ensure()
	days := s.retentionDays()
	report := map[string]any{"retention_days": days, "results": []map[string]any{}, "partial_failure": false}
	if days == 0 {
		return report, nil
	}
	var clusters []map[string]any
	var archives []string
	if name != "" {
		cluster, err := s.Registry.Get(name)
		if err != nil {
			return nil, err
		}
		clusters = []map[string]any{cluster}
		if archive := s.archive(cluster); archive != "" {
			archives = []string{archive}
		}
	} else {
		var err error
		clusters, err = s.Registry.List()
		if err != nil {
			return nil, err
		}
		archives = collectedArchives(filepath.Join(filepath.Dir(s.Registry.Path), "collected"))
	}
	minutes := days * 24 * 60
	cutoff := time.Now().Add(-time.Duration(minutes) * time.Minute)
	results := make([]map[string]any, len(clusters)+len(archives))
	runBatches(len(clusters), func(i int) {
		removed, err := s.purgeTarget(ctx, clusters[i], cutoff)
		results[i] = purgeResult(stringField(clusters[i]["id"]), "target", removed, err)
	})
	for i, archive := range archives {
		removed, err := purgeArchive(ctx, archive, cutoff)
		results[len(clusters)+i] = purgeResult(filepath.Base(archive), "archive", removed, err)
	}
	for _, result := range results {
		if result["error"] != nil {
			report["partial_failure"] = true
		}
	}
	report["results"] = results
	return report, nil
}

func purgeResult(clusterID, location string, removed []string, err error) map[string]any {
	if removed == nil {
		removed = []string{}
	}
	result := map[string]any{"cluster_id": clusterID, "location": location, "removed": removed}
	if err != nil {
		result["error"] = asData(err)
	}
	return result
}

// purgeTarget only considers directories with an observed identity, then
// queries that identity live before removing anything.
func (s *Service) purgeTarget(ctx context.Context, cluster map[string]any, cutoff time.Time) ([]string, error) {
	if clusterTransport(cluster)["kind"] == "ssh" {
		// Sources are removed by the verified collection transaction.
		return nil, nil
	}
	id := stringField(cluster["id"])
	root, err := s.jobRoot(ctx, cluster)
	if err != nil {
		return nil, err
	}
	_, item, err := s.cluster(id)
	if err != nil {
		return nil, err
	}
	release, err := s.acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	defer release()
	return purgeDirectories(ctx, root, cutoff, func(directory string) (bool, error) {
		proof, err := os.ReadFile(directory + ".terminal.json")
		if os.IsNotExist(err) {
			return false, nil
		}
		var ref map[string]any
		if err != nil || json.Unmarshal(proof, &ref) != nil || ref["cluster_id"] != id || s.checkRef(ref) != nil {
			return false, nil
		}
		jobs, err := item.QueryMany(ctx, []string{stringField(ref["native_id"])})
		if err != nil {
			return false, err
		}
		job := jobs[stringField(ref["native_id"])]
		if !scheduler.Terminal(job.State) || job.Incarnation != ref["incarnation"] || job.Workdir == nil ||
			filepath.Clean(*job.Workdir) != directory {
			return false, nil
		}
		return true, nil
	}, ".terminal.json", false)
}

func (s *Service) rememberTerminalDirectory(ctx context.Context, cluster map[string]any, job scheduler.Job) {
	if clusterTransport(cluster)["kind"] == "ssh" || collectable(job, "") == "" {
		return
	}
	root, err := s.jobRoot(ctx, cluster)
	if err != nil {
		return
	}
	dir := filepath.Clean(*job.Workdir)
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || !managedJobPath(filepath.ToSlash(root), filepath.ToSlash(dir)) {
		return
	}
	ref, err := s.ref(cluster, job)
	if err == nil {
		_ = writeReceipt(dir+".terminal.json", ref, true)
	}
}

// collectedArchives lists the per-Cluster directories in the serve-host
// archive, including those of Clusters removed since.
func collectedArchives(root string) []string {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			out = append(out, filepath.Join(root, entry.Name()))
		}
	}
	return out
}

func purgeArchive(ctx context.Context, root string, cutoff time.Time) ([]string, error) {
	return purgeDirectories(ctx, root, cutoff, func(directory string) (bool, error) {
		receipt, err := readCollectionReceipt(directory)
		if err != nil || !receipt.Cleaned || path.Base(receipt.Remote) != scheduler.ControlDirectory {
			return false, nil
		}
		entries, err := collectionManifest(ctx, directory, false)
		return err == nil && reflect.DeepEqual(entries, receipt.Entries) && ownedEntries(directory, entries), err
	}, ".receipt.json", true)
}

func purgeDirectories(ctx context.Context, root string, cutoff time.Time, confirmed func(string) (bool, error), sidecar string, archive bool) ([]string, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read the job directory archive", map[string]any{"path": root})
	}
	var removed, failed []string
	remove := func(directory, identity string) {
		control := directory
		if !archive {
			control = filepath.Join(directory, scheduler.ControlDirectory)
		}
		if !isDirectory(control) {
			return
		}
		newest, err := newestModification(control)
		if err != nil {
			failed = append(failed, identity)
			return
		}
		if newest.After(cutoff) {
			return
		}
		safe, err := confirmed(directory)
		if err != nil {
			failed = append(failed, identity)
			return
		}
		if !safe {
			return
		}
		if newest, err := newestModification(control); err != nil || newest.After(cutoff) {
			return
		}
		entries, err := collectionManifest(ctx, control, false)
		if err != nil || !ownedEntries(control, entries) {
			return
		}
		if err := removeOwnedLocal(control, entries); err != nil {
			failed = append(failed, identity)
			return
		}
		_ = os.Remove(directory + sidecar)
		if !archive {
			_ = os.Remove(directory)
		}
		removed = append(removed, identity)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || (!archive && !jobIdentity.MatchString(name)) || (archive && !archiveIdentity(name)) {
			continue
		}
		directory := filepath.Join(root, name)
		remove(directory, name)
		if archive {
			continue
		}
		children, err := os.ReadDir(directory)
		if err != nil {
			continue
		}
		for _, child := range children {
			if child.IsDir() && arrayDirectory(child.Name()) {
				remove(filepath.Join(directory, child.Name()), name+"_"+child.Name())
			}
		}

	}
	if len(failed) > 0 {
		sort.Strings(failed)
		return removed, protocol.Fail("FILE_UNAVAILABLE", "Some job directories could not be purged", map[string]any{"path": root, "directories": failed})
	}
	return removed, nil
}

// newestModification checks the whole tree without following symlinks.
func newestModification(directory string) (time.Time, error) {
	var newest time.Time
	err := filepath.WalkDir(directory, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		item, err := entry.Info()
		if err == nil && item.ModTime().After(newest) {
			newest = item.ModTime()
		}
		return err
	})
	return newest, err
}

func removeOwnedLocal(directory string, entries map[string]string) error {
	for name, sum := range entries {
		if name == "." || name == scheduler.ControlManifest {
			continue
		}
		file := filepath.Join(directory, name)
		info, err := os.Lstat(file)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || !strings.HasPrefix(sum, "sha256:") {
			return fmt.Errorf("control file changed: %s", name)
		}
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(directory, scheduler.ControlManifest)); err != nil {
		return err
	}
	return os.Remove(directory)
}
