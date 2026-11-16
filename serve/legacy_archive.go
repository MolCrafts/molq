package serve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

const legacyOwner = "owned-v1"

type collectionReceipt struct {
	Remote  string            `json:"remote"`
	Binding string            `json:"binding"`
	Entries map[string]string `json:"entries"`
	Cleaned bool              `json:"cleaned"`
}

func readCollectionReceipt(root *os.Root, name string) (collectionReceipt, error) {
	var receipt collectionReceipt
	file, err := root.Open(name + ".receipt.json")
	if err != nil {
		return receipt, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return receipt, fmt.Errorf("cannot read collection receipt")
	}
	if err := json.Unmarshal(data, &receipt); err != nil || receipt.Remote == "" || receipt.Binding == "" || receipt.Entries["."] != "directory" {
		return receipt, fmt.Errorf("invalid collection receipt")
	}
	return receipt, nil
}

func collectionManifest(ctx context.Context, root *os.Root) (map[string]string, error) {
	entries := map[string]string{}
	err := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		switch {
		case entry.IsDir():
			entries[name] = "directory"
		case entry.Type()&os.ModeSymlink != 0:
			target, err := root.Readlink(name)
			if err != nil {
				return err
			}
			entries[name] = "symlink:" + target
		case entry.Type().IsRegular():
			file, err := root.Open(name)
			if err != nil {
				return err
			}
			hash := sha256.New()
			_, err = io.Copy(hash, contextReader{ctx, file})
			_ = file.Close()
			if err != nil {
				return err
			}
			entries[name] = "sha256:" + hex.EncodeToString(hash.Sum(nil))
		default:
			return fmt.Errorf("unsupported file type: %s", name)
		}
		return nil
	})
	return entries, err
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func collectionRsync(ctx context.Context, binding map[string]any, remote, local string) error {
	source := location{transport: binding, path: remote + "/", root: remote}
	destination := location{transport: map[string]any{"kind": "local"}, path: filepath.ToSlash(local) + "/"}
	runner, rsh, operands, err := transferRoute([]location{source}, destination)
	if err != nil {
		return err
	}
	args := []string{"rsync", "-rl", "--checksum", "--delete", "--dry-run", "--itemize-changes"}
	args = append(append(args, rsh...), "--")
	result, err := runner.Run(ctx, append(args, operands...), transferTimeout, nil, false)
	if err != nil {
		return err
	}
	if result.ReturnCode != 0 || len(result.Stderr) != 0 || collectionDifferences(result.Text()) {
		return protocol.Fail("FILE_UNAVAILABLE", "Job directory copy or verification failed", map[string]any{
			"path": remote, "returncode": result.ReturnCode,
			"stderr": strings.ToValidUTF8(string(result.Stderr), "�"), "differences": result.Text(),
		})
	}
	return nil
}

// A dry run may report metadata-only changes for rsync implementations
// that preserve destination timestamps differently. Compare bytes and names.
func collectionDifferences(output string) bool {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || len(fields[0]) < 9 {
			return true
		}
		item := fields[0]
		if item[0] != '.' || item[2] != '.' || item[3] != '.' {
			return true
		}
	}
	return false
}

func ownedEntries(directory *os.Root, entries map[string]string) bool {
	info, err := directory.Lstat(legacyOwner)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 128 {
		return false
	}
	body, err := fs.ReadFile(directory.FS(), legacyOwner)
	if err != nil || entries[legacyOwner] == "" {
		return false
	}
	owned := strings.TrimSuffix(string(body), "\n")
	if owned != "stdout stderr" && owned != "stdout stderr job.sh" && owned != "stdout stderr job.sh launch exit cancel_requested" {
		return false
	}
	names := strings.Fields(owned)
	for name, sum := range entries {
		if name == "." {
			continue
		}
		if !strings.HasPrefix(sum, "sha256:") || (name != legacyOwner && !slices.Contains(names, name)) {
			return false
		}
	}
	return true
}

var archivedClusterID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var legacyArchiveName = regexp.MustCompile(`^[0-9a-f]{32}(?:_[0-9]+)?$`)

const legacyArchivePage = 128

func (s *Service) legacyArchives(ctx context.Context, params map[string]any) (any, error) {
	key := stringField(params["cluster"])
	cluster, err := s.Registry.Get(key)
	id := stringField(cluster["id"])
	if err != nil {
		item, ok := err.(*protocol.Error)
		if !ok || item.Kind != "CLUSTER_NOT_FOUND" || !archivedClusterID.MatchString(key) || params["entries"] != nil {
			return nil, err
		}
		id = key // The retired cluster's files can still be inventoried.
	}
	if !archivedClusterID.MatchString(id) {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid archive cluster identity", nil)
	}
	release, err := s.acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	defer release()
	base := filepath.Join(filepath.Dir(s.Registry.Path), "collected")
	directory := filepath.Join(base, id)
	report := []map[string]any{}
	for _, name := range []string{base, directory} {
		info, err := os.Lstat(name)
		if os.IsNotExist(err) {
			return map[string]any{"entries": report}, nil
		}
		if err != nil || !info.IsDir() {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot open legacy archive directory", map[string]any{"path": name})
		}
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	names := []string{}
	selection := anySlice(params["entries"])
	if len(selection) != 0 {
		for _, item := range selection {
			name := stringField(item)
			if !legacyArchiveName.MatchString(name) {
				return nil, protocol.Fail("INVALID_INPUT", "Invalid legacy archive identity", map[string]any{"name": name})
			}
			names = append(names, name)
		}
	} else {
		entries, err := fs.ReadDir(root.FS(), ".")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if name <= stringField(params["after"]) {
				continue
			}
			if strings.HasSuffix(name, ".receipt.json") {
				if _, err := root.Lstat(strings.TrimSuffix(name, ".receipt.json")); err == nil {
					continue
				}
			}
			names = append(names, name)
		}
	}
	next := ""
	if len(names) > legacyArchivePage {
		names = names[:legacyArchivePage]
		next = names[len(names)-1]
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, protocol.Fail("TRANSPORT_UNAVAILABLE", "Legacy archive scan timed out", nil)
		}
		item := map[string]any{"name": name, "path": filepath.Join(directory, name)}
		receipt, status, size := checkLegacyArchive(ctx, clusterTransport(cluster), root, name, len(selection) != 0)
		item["status"], item["bytes"] = status, size
		if len(selection) != 0 && status == "duplicate" {
			item["status"] = "removed"
			// The manifest contains only regular files directly under this run.
			// Remove individually so unexpected entries can never be removed.
			run, err := root.OpenRoot(name)
			if err != nil {
				return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot reopen verified legacy archive", item)
			}
			err = removeVerifiedEntries(ctx, run, receipt.Entries)
			run.Close()
			if err != nil {
				return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Could not complete legacy archive removal", "unknown", item)
			}
			if err := root.Remove(name); err != nil {
				return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Could not remove verified legacy archive directory", "unknown", item)
			}
			if err := root.Remove(name + ".receipt.json"); err != nil {
				return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Could not remove legacy archive receipt", "unknown", item)
			}
		}
		report = append(report, item)
	}
	result := map[string]any{"entries": report}
	if next != "" {
		result["after"] = next
	}
	return result, nil
}

func removeVerifiedEntries(ctx context.Context, root *os.Root, expected map[string]string) error {
	current, err := collectionManifest(ctx, root)
	if err != nil || !maps.Equal(current, expected) {
		return fmt.Errorf("legacy archive changed after verification")
	}
	for name := range expected {
		if name != "." {
			if err := root.Remove(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkLegacyArchive(ctx context.Context, binding map[string]any, root *os.Root, name string, verifyRemote bool) (collectionReceipt, string, int64) {
	var receipt collectionReceipt
	if !legacyArchiveName.MatchString(name) {
		return receipt, "unverified", 0
	}
	if info, err := root.Lstat(name); err != nil || !info.IsDir() {
		return receipt, "unverified", 0
	}
	sidecar := name + ".receipt.json"
	if info, err := root.Lstat(sidecar); err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return receipt, "unverified", 0
	}
	directory, err := root.OpenRoot(name)
	if err != nil {
		return receipt, "unverified", 0
	}
	defer directory.Close()
	receipt, err = readCollectionReceipt(root, name)
	remoteRun := path.Dir(receipt.Remote)
	parent := path.Dir(remoteRun)
	expected := path.Base(remoteRun)
	if !strings.HasSuffix(parent, "/.molcrafts/molq/jobs") {
		expected = path.Base(parent) + "_" + expected
		parent = path.Dir(parent)
	}
	if err != nil || path.Base(receipt.Remote) != ".molq-control" || path.Clean(receipt.Remote) != receipt.Remote ||
		!path.IsAbs(receipt.Remote) || strings.ContainsAny(receipt.Remote, "\r\n\x00") ||
		!strings.HasSuffix(parent, "/.molcrafts/molq/jobs") || expected != name {
		return receipt, "unverified", 0
	}
	entries, err := collectionManifest(ctx, directory)
	if err != nil || !maps.Equal(entries, receipt.Entries) || !ownedEntries(directory, entries) {
		return receipt, "unverified", 0
	}
	var size int64
	for entry := range entries {
		if entry != "." {
			info, err := directory.Lstat(entry)
			if err != nil || !info.Mode().IsRegular() {
				return receipt, "unverified", 0
			}
			size += info.Size()
		}
	}
	if binding["kind"] != "ssh" || receipt.Binding != fingerprint("collection", binding) {
		return receipt, "unverified_target", size
	}
	if !verifyRemote {
		if receipt.Cleaned {
			return receipt, "source_was_cleaned", size
		}
		return receipt, "verified_archive", size
	}
	target := transport.Create(binding)
	remote, err := resolveFilePath(ctx, target, receipt.Remote, parent)
	if err != nil || remote != receipt.Remote {
		return receipt, "no_verified_duplicate", size
	}
	// rsync's checksum verifies contents it can transfer. Independently reject
	// links, special files and unexpected names before allowing any deletion.
	allowed := make([]string, 0, len(entries)-1)
	for name := range entries {
		if name != "." {
			allowed = append(allowed, name)
		}
	}
	out, err := runFileScript(ctx, target, false, remote, `root=$1; shift
[ -d "$root" ] && [ ! -L "$root" ] || exit 1
for name do
  [ -f "$root/$name" ] && [ ! -L "$root/$name" ] || exit 1
done
for file in "$root"/* "$root"/.[!.]* "$root"/..?*; do
  [ -e "$file" ] || [ -L "$file" ] || continue
  match=
  for name do
    if [ "${file##*/}" = "$name" ]; then match=y; break; fi
  done
  [ "$match" = y ] || exit 1
done
printf verified`, append([]string{remote}, allowed...)...)
	if err != nil || string(out) != "verified" || collectionRsync(ctx, binding, remote, directory.Name()) != nil {
		return receipt, "no_verified_duplicate", size
	}
	return receipt, "duplicate", size
}
