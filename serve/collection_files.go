package serve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"

	"github.com/MolCrafts/molq/scheduler"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

type collectionReceipt struct {
	Remote  string            `json:"remote"`
	Binding string            `json:"binding"`
	Entries map[string]string `json:"entries"`
	Cleaned bool              `json:"cleaned"`
}

func receiptPath(final string) string { return final + ".receipt.json" }

func readCollectionReceipt(final string) (collectionReceipt, error) {
	var receipt collectionReceipt
	data, err := os.ReadFile(receiptPath(final))
	if err == nil {
		err = json.Unmarshal(data, &receipt)
		if err == nil && (receipt.Remote == "" || receipt.Binding == "" || receipt.Entries["."] != "directory") {
			err = fmt.Errorf("invalid collection receipt")
		}
	}
	return receipt, err
}

func collectionNeedsCleanup(final string) bool {
	receipt, err := readCollectionReceipt(final)
	return err == nil && path.Base(receipt.Remote) == scheduler.ControlDirectory && !receipt.Cleaned
}

// writeReceipt publishes complete JSON and syncs it before destructive work.
func writeReceipt(name string, value any, exclusive bool) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(name), ".molq-receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(body)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if exclusive {
		err = os.Link(file.Name(), name)
	} else {
		err = os.Rename(file.Name(), name)
	}
	if err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(name))
}

func syncDirectory(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return syncDirectoryHandle(dir)
}

func collectionManifest(ctx context.Context, root string, durable bool) (map[string]string, error) {
	entries := map[string]string{}
	var directories []string
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		switch {
		case entry.IsDir():
			entries[key] = "directory"
			directories = append(directories, name)
		case entry.Type()&os.ModeSymlink != 0:
			target, err := os.Readlink(name)
			if err != nil {
				return err
			}
			entries[key] = "symlink:" + target
		case entry.Type().IsRegular():
			file, err := os.Open(name)
			if err != nil {
				return err
			}
			hash := sha256.New()
			_, err = io.Copy(hash, contextReader{ctx, file})
			if err == nil && durable {
				err = file.Sync()
			}
			_ = file.Close()
			if err != nil {
				return err
			}
			entries[key] = "sha256:" + hex.EncodeToString(hash.Sum(nil))
		default:
			return fmt.Errorf("unsupported file type: %s", name)
		}
		return nil
	})
	if err == nil && durable {
		for i := len(directories) - 1; i >= 0; i-- {
			if err = syncDirectory(directories[i]); err != nil {
				break
			}
		}
	}
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

func collectionRsync(ctx context.Context, binding map[string]any, remote, local string, verify, subset bool) error {
	source := location{transport: binding, path: remote + "/", root: remote}
	destination := location{transport: map[string]any{"kind": "local"}, path: filepath.ToSlash(local) + "/"}
	runner, rsh, operands, err := transferRoute([]location{source}, destination)
	if err != nil {
		return err
	}
	args := []string{"rsync", "-rl", "--checksum"}
	if !subset {
		args = append(args, "--delete")
	}
	if !verify {
		args = append(args, "-pt")
	}
	if verify {
		args = append(args, "--dry-run", "--itemize-changes")
	} else {
		args = append(args, "--partial")
	}
	args = append(append(args, rsh...), "--")
	result, err := runner.Run(ctx, append(args, operands...), transferTimeout, nil, false)
	if err != nil {
		return err
	}
	if result.ReturnCode != 0 || len(result.Stderr) != 0 || (verify && collectionDifferences(result.Text())) {
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

// ownedControl validates the ownership marker and every existing entry. An
// unexpected entry disables cleanup for the whole control directory.
const ownedControl = `[ -d "$1" ] && [ ! -L "$1" ] && [ ! -L "${1%/*}" ] || exit 3
marker=$2
[ ! -L "$1/$marker" ] && [ -f "$1/$marker" ] || exit 3
IFS= read -r owned < "$1/$marker" || exit 3
case "$owned" in
  'stdout stderr'|'stdout stderr job.sh'|'stdout stderr job.sh launch exit cancel_requested') ;;
  *) exit 3 ;;
esac
for f in "$1"/* "$1"/.[!.]* "$1"/..?*; do
  [ -e "$f" ] || [ -L "$f" ] || continue
  [ -f "$f" ] && [ ! -L "$f" ] || exit 3
  name=${f##*/}
  [ "$name" = "$marker" ] && continue
  case " $owned " in *" $name "*) ;; *) exit 3;; esac
done
`

func ownedEntries(directory string, entries map[string]string) bool {
	marker := filepath.Join(directory, scheduler.ControlManifest)
	info, err := os.Lstat(marker)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 128 {
		return false
	}
	body, err := os.ReadFile(marker)
	if err != nil || entries[scheduler.ControlManifest] == "" {
		return false
	}
	owned := strings.TrimSuffix(string(body), "\n")
	if owned != "stdout stderr" && owned != "stdout stderr job.sh" && owned != scheduler.ControlFiles {
		return false
	}
	names := strings.Fields(owned)
	for name, sum := range entries {
		if name == "." {
			continue
		}
		if !strings.HasPrefix(sum, "sha256:") || (name != scheduler.ControlManifest && !slices.Contains(names, name)) {
			return false
		}
	}
	return true
}

func (s *Service) moveJobDirectory(ctx context.Context, cluster map[string]any, remote, final string) error {
	if path.Base(remote) != scheduler.ControlDirectory {
		return nil
	}
	binding := clusterTransport(cluster)
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return err
	}
	// Persist the archive's ancestry before deleting anything on the target.
	for directory := filepath.Dir(filepath.Dir(final)); ; directory = filepath.Dir(directory) {
		if err := syncDirectory(directory); err != nil {
			return err
		}
		if directory == filepath.Dir(s.Registry.Path) || directory == filepath.Dir(directory) {
			break
		}
	}
	if !isDirectory(final) {
		target := transport.Create(binding)
		check, err := target.Run(ctx, []string{"sh", "-c", ownedControl, "sh", remote, scheduler.ControlManifest}, 30*time.Second, nil, false)
		if err != nil {
			return err
		}
		if check.ReturnCode != 0 {
			return nil
		}
		if _, err := os.Lstat(filepath.Join(filepath.Dir(final), "."+filepath.Base(final)+".partial")); !os.IsNotExist(err) {
			return protocol.Fail("FILE_UNAVAILABLE", "Legacy partial job directory requires manual recovery", nil)
		}
		stage := final + ".staging"
		if err := os.Mkdir(stage, 0o700); err != nil && !(os.IsExist(err) && isDirectory(stage)) {
			return err
		}
		if err := collectionRsync(ctx, binding, remote, stage, false, false); err != nil {
			return err
		}
		if err := collectionRsync(ctx, binding, remote, stage, true, false); err != nil {
			return err
		}
		entries, err := collectionManifest(ctx, stage, true)
		if err != nil {
			return err
		}
		if !ownedEntries(stage, entries) {
			return protocol.Fail("FILE_UNAVAILABLE", "Staged files do not match the ownership manifest", nil)
		}
		receipt := collectionReceipt{Remote: remote, Binding: fingerprint("collection", binding), Entries: entries}
		if err := writeReceipt(receiptPath(final), receipt, false); err != nil {
			return err
		}
		if err := os.Rename(stage, final); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(final)); err != nil {
			return err
		}
	}
	receipt, err := readCollectionReceipt(final)
	if err != nil {
		return err
	}
	if receipt.Cleaned {
		return nil
	}
	if receipt.Remote != remote || receipt.Binding != fingerprint("collection", binding) {
		return protocol.Fail("FILE_UNAVAILABLE", "Collection source does not match its receipt", nil)
	}
	entries, err := collectionManifest(ctx, final, false)
	if err != nil || !reflect.DeepEqual(entries, receipt.Entries) || !ownedEntries(final, entries) {
		return protocol.Fail("FILE_UNAVAILABLE", "Committed job directory no longer matches its receipt", nil)
	}
	if err := finishCollection(ctx, binding, remote, final); err != nil {
		return err
	}
	receipt.Cleaned = true
	return writeReceipt(receiptPath(final), receipt, false)
}

func finishCollection(ctx context.Context, binding map[string]any, remote, final string) error {
	target := transport.Create(binding)
	run := func(script string) (transport.Result, error) {
		return target.Run(ctx, []string{"sh", "-c", script, "sh", remote, scheduler.ControlManifest}, 30*time.Second, nil, false)
	}
	check, err := run(`[ ! -L "${1%/*}" ] && [ ! -L "$1" ] || exit 3
if [ ! -e "$1" ]; then printf absent
elif [ ! -e "$1/$2" ] && [ ! -L "$1/$2" ]; then
  rmdir -- "$1" || exit 3
  printf absent
else printf present; fi`)
	if err != nil {
		return err
	}
	if check.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Unexpected collection source", nil)
	}
	if check.Text() == "absent" {
		return nil
	}
	check, err = run(ownedControl)
	if err != nil {
		return err
	}
	if check.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Source no longer matches its ownership manifest", nil)
	}
	if err := collectionRsync(ctx, binding, remote, final, true, true); err != nil {
		return err
	}
	receipt, err := readCollectionReceipt(final)
	if err != nil {
		return err
	}
	// Only names actually committed can be removed, even after a partial cleanup.
	names := ""
	for name := range receipt.Entries {
		if name != "." && name != scheduler.ControlManifest {
			names += " " + name
		}
	}
	deleted, err := target.Run(ctx, []string{"sh", "-c", ownedControl + `
for f in "$1"/*; do
  [ -e "$f" ] || continue
  name=${f##*/}
  [ "$name" = "$2" ] && continue
  case " $3 " in *" $name "*) ;; *) exit 3;; esac
done
for name in $3; do
  f=$1/$name
  [ ! -L "$f" ] || exit 3
  if [ -e "$f" ]; then [ -f "$f" ] && rm -- "$f" || exit 3; fi
done
rm -- "$1/$2" && rmdir -- "$1"
`, "sh", remote, scheduler.ControlManifest, names}, 30*time.Second, nil, false)
	if err != nil {
		return err
	}
	if deleted.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot remove verified control files", nil)
	}
	return nil
}
