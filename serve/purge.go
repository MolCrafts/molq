package serve

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

const (
	// defaultRetentionDays applies when settings.job_retention_days is absent.
	defaultRetentionDays = 30
	// PurgeInterval is how often a listening serve process purges.
	PurgeInterval = 24 * time.Hour
	purgeTimeout  = 120 * time.Second
	purgeDeadline = 10 * time.Minute
)

// purgeTree deletes job directories under $1 whose directory and immediate
// entries were all last modified at least $2 minutes ago, and prints each
// deleted name. Only names that are a 32-digit lowercase hexadecimal identity
// are considered; symlinks and everything else under the root stay.
const purgeTree = `[ -e "$1" ] || exit 0
cd -- "$1" || exit 1
status=0
for name in *; do
  case "$name" in *[!0123456789abcdef]*) continue ;; esac
  [ "${#name}" -eq 32 ] && [ -d "$name" ] && [ ! -h "$name" ] || continue
  recent=$(find "$name" -maxdepth 1 -mmin -"$2" -print) || { status=1; continue; }
  [ -z "$recent" ] || continue
  if rm -rf -- "$name"; then printf '%s\n' "$name"; else status=1; fi
done
exit "$status"`

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
		removed, err := s.purgeTarget(ctx, clusters[i], minutes)
		results[i] = purgeResult(stringField(clusters[i]["id"]), "target", removed, err)
	})
	for i, archive := range archives {
		removed, err := purgeLocal(archive, cutoff)
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

// purgeTarget runs purgeTree in the Cluster's job root through its Transport,
// holding the Cluster's native slot like any other target command.
func (s *Service) purgeTarget(ctx context.Context, cluster map[string]any, minutes int) ([]string, error) {
	root, err := s.jobRoot(ctx, cluster)
	if err != nil {
		return nil, err
	}
	release, err := s.acquire(ctx, stringField(cluster["id"]))
	if err != nil {
		return nil, err
	}
	defer release()
	result, err := transport.Create(clusterTransport(cluster)).Run(ctx, []string{"sh", "-c", purgeTree, "sh", root, strconv.Itoa(minutes)}, purgeTimeout, nil, false)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, line := range strings.Split(result.Text(), "\n") {
		if jobIdentity.MatchString(line) {
			removed = append(removed, line)
		}
	}
	if result.ReturnCode != 0 {
		return removed, protocol.Fail("FILE_UNAVAILABLE", "Some job directories could not be purged", map[string]any{
			"path": root, "returncode": result.ReturnCode,
			"stderr": strings.ToValidUTF8(string(result.Stderr), "�"),
		})
	}
	return removed, nil
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

// purgeLocal is purgeTree for a serve-host directory: it deletes each job
// directory under root whose directory and immediate entries were all last
// modified before cutoff.
func purgeLocal(root string, cutoff time.Time) ([]string, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read the job directory archive", map[string]any{"path": root})
	}
	var removed, failed []string
	for _, entry := range entries {
		if !entry.IsDir() || !jobIdentity.MatchString(entry.Name()) {
			continue
		}
		directory := filepath.Join(root, entry.Name())
		newest, err := newestModification(directory)
		if err != nil {
			failed = append(failed, entry.Name())
			continue
		}
		if newest.After(cutoff) {
			continue
		}
		if err := os.RemoveAll(directory); err != nil {
			failed = append(failed, entry.Name())
			continue
		}
		removed = append(removed, entry.Name())
	}
	if len(failed) > 0 {
		sort.Strings(failed)
		return removed, protocol.Fail("FILE_UNAVAILABLE", "Some job directories could not be purged", map[string]any{"path": root, "directories": failed})
	}
	return removed, nil
}

// newestModification is the latest modification time of directory and its
// immediate entries. Symlinks report their own time.
func newestModification(directory string) (time.Time, error) {
	info, err := os.Lstat(directory)
	if err != nil {
		return time.Time{}, err
	}
	newest := info.ModTime()
	entries, err := os.ReadDir(directory)
	if err != nil {
		return time.Time{}, err
	}
	for _, entry := range entries {
		item, err := entry.Info()
		if err != nil {
			return time.Time{}, err
		}
		if item.ModTime().After(newest) {
			newest = item.ModTime()
		}
	}
	return newest, nil
}
