package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/MolCrafts/molq/protocol"
)

var registryDirectoryName = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var legacySubmissionName = regexp.MustCompile(`^([0-9a-f]{64})\.json(?:\.(?:result|native-id))?$`)

// legacySubmissions inventories the exact files to import. Old requests are
// retired under the new contract, including records with missing sidecars.
func (r *Registry) legacySubmissions() (map[string]map[string][]byte, error) {
	root := filepath.Join(filepath.Dir(r.Path), "submissions", r.ID)
	// The registry identity is used as a local path component.
	if !registryDirectoryName.MatchString(r.ID) {
		return nil, fmt.Errorf("invalid registry identity")
	}
	dir, err := os.OpenRoot(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	for _, name := range []string{filepath.Dir(root), root} {
		info, err := os.Lstat(name)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("invalid submission directory: %s", name)
		}
	}
	entries, err := fs.ReadDir(dir.FS(), ".")
	if err != nil {
		return nil, err
	}
	records := map[string]map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".molq-receipt-") && entry.Type().IsRegular() {
			// A temporary inode has no key and was never the dispatch marker.
			continue
		}
		match := legacySubmissionName.FindStringSubmatch(name)
		if match == nil || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("unrecognized submission evidence: %s", name)
		}
		body, err := readLegacyFile(dir, name)
		if err != nil {
			return nil, err
		}
		if records[match[1]] == nil {
			records[match[1]] = map[string][]byte{}
		}
		records[match[1]][name] = body
	}
	return records, nil
}

func readLegacyFile(dir *os.Root, name string) ([]byte, error) {
	file, err := dir.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	const max = 4 << 20
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > max {
		return nil, fmt.Errorf("invalid submission evidence: %s", name)
	}
	body, err := io.ReadAll(io.LimitReader(file, max+1))
	if len(body) > max {
		return nil, fmt.Errorf("submission evidence is too large: %s", name)
	}
	return body, err
}

func (r *Registry) migrateSubmissions() error {
	records, err := r.legacySubmissions()
	if err != nil {
		return migrationFailure(err)
	}
	tx, err := r.db.Begin()
	if err != nil {
		return migrationFailure(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(submissionTable); err != nil {
		return migrationFailure(err)
	}
	for digest, files := range records {
		body, err := json.Marshal(files)
		if err != nil {
			return migrationFailure(err)
		}
		if _, err := tx.Exec("INSERT INTO submission_operations VALUES (?, ?, 1, 1)", digest, string(body)); err != nil {
			return migrationFailure(err)
		}
	}
	if _, err := tx.Exec("UPDATE schema_meta SET value=? WHERE key='schema_version'", schemaVersion); err != nil {
		return migrationFailure(err)
	}
	if err := tx.Commit(); err != nil {
		return migrationFailure(err)
	}
	return nil
}

// PruneImportedSubmissions removes only files byte-for-byte imported into SQLite.
func (r *Registry) PruneImportedSubmissions(ctx context.Context) (removed, retained []string, err error) {
	records, err := r.legacySubmissions()
	if err != nil {
		return nil, nil, migrationFailure(err)
	}
	removed, retained = []string{}, []string{}
	if len(records) == 0 {
		return removed, retained, nil
	}
	dir, err := os.OpenRoot(filepath.Join(filepath.Dir(r.Path), "submissions", r.ID))
	if err != nil {
		return nil, nil, migrationFailure(err)
	}
	defer dir.Close()
	for digest, files := range records {
		if err := ctx.Err(); err != nil {
			return removed, retained, err
		}
		row, legacy, err := r.ReadSubmission(ctx, digest)
		var imported map[string][]byte
		verified := err == nil && legacy && json.Unmarshal(row, &imported) == nil
		for name, original := range files {
			expected, exists := imported[name]
			info, statErr := dir.Lstat(name)
			if !verified || !exists || !bytes.Equal(expected, original) ||
				statErr != nil || !info.Mode().IsRegular() {
				retained = append(retained, name)
				continue
			}
			current, readErr := readLegacyFile(dir, name)
			if readErr != nil || !bytes.Equal(current, original) || dir.Remove(name) != nil {
				retained = append(retained, name)
				continue
			}
			removed = append(removed, name)
		}
	}
	slices.Sort(removed)
	slices.Sort(retained)
	return removed, retained, nil
}

func migrationFailure(err error) error {
	return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", "Cannot migrate legacy submission evidence",
		map[string]any{"detail": err.Error()})
}
