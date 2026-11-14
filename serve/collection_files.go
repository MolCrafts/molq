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
	return err == nil && !receipt.Cleaned
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

func collectionRsync(ctx context.Context, binding map[string]any, remote, local string, verify bool) error {
	source := location{transport: binding, path: remote + "/"}
	destination := location{transport: map[string]any{"kind": "local"}, path: filepath.ToSlash(local) + "/"}
	runner, rsh, operands, err := transferRoute([]location{source}, destination)
	if err != nil {
		return err
	}
	args := []string{"rsync", "-rlpt", "--checksum", "--delete"}
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
	if result.ReturnCode != 0 || len(result.Stderr) != 0 || (verify && strings.TrimSpace(result.Text()) != "") {
		return protocol.Fail("FILE_UNAVAILABLE", "Job directory copy or verification failed", map[string]any{
			"path": remote, "returncode": result.ReturnCode,
			"stderr": strings.ToValidUTF8(string(result.Stderr), "�"),
		})
	}
	return nil
}

func (s *Service) moveJobDirectory(ctx context.Context, cluster map[string]any, remote, final string) error {
	release, err := s.acquire(ctx, stringField(cluster["id"]))
	if err != nil {
		return err
	}
	defer release()
	binding := clusterTransport(cluster)
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return err
	}
	if !isDirectory(final) {
		if _, err := os.Lstat(filepath.Join(filepath.Dir(final), "."+filepath.Base(final)+".partial")); !os.IsNotExist(err) {
			return protocol.Fail("FILE_UNAVAILABLE", "Legacy partial job directory requires manual recovery", nil)
		}
		stage := final + ".staging"
		if err := os.MkdirAll(stage, 0o700); err != nil {
			return err
		}
		if err := collectionRsync(ctx, binding, remote, stage, false); err != nil {
			return err
		}
		if err := collectionRsync(ctx, binding, remote, stage, true); err != nil {
			return err
		}
		entries, err := collectionManifest(ctx, stage, true)
		if err != nil {
			return err
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
	if err != nil || !reflect.DeepEqual(entries, receipt.Entries) {
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
	// Move the source aside only after comparing it with the durable copy.
	// A separate name also lets a later observation resume after interruption.
	quarantine := remote + ".collected"
	run := func(script string) (transport.Result, error) {
		return target.Run(ctx, []string{"sh", "-c", script, "sh", remote, quarantine}, 30*time.Second, nil, false)
	}
	check, err := run(`for p in "$1" "$2"; do [ ! -L "$p" ] || exit 3; done
if [ -d "$2" ]; then [ ! -e "$1" ] || exit 3; printf quarantine
elif [ -d "$1" ]; then printf source
elif [ ! -e "$1" ] && [ ! -e "$2" ]; then printf absent
else exit 3; fi`)
	if err != nil {
		return err
	}
	if check.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Unexpected collection source", nil)
	}
	if check.Text() == "absent" {
		return nil
	}
	if check.Text() == "source" {
		if err := collectionRsync(ctx, binding, remote, final, true); err != nil {
			return err
		}
		moved, err := run(`[ -d "$1" ] && [ ! -L "$1" ] && [ ! -e "$2" ] && [ ! -L "$2" ] && mv -- "$1" "$2"`)
		if err != nil {
			return err
		}
		if moved.ReturnCode != 0 {
			return protocol.Fail("FILE_UNAVAILABLE", "Cannot isolate verified collection source", nil)
		}
	} else if check.Text() != "quarantine" {
		return protocol.Fail("FILE_UNAVAILABLE", "Unexpected collection source status", nil)
	}
	if err := collectionRsync(ctx, binding, quarantine, final, true); err != nil {
		return err
	}
	deleted, err := run(`[ ! -e "$1" ] && [ ! -L "$1" ] && [ -d "$2" ] && [ ! -L "$2" ] && rm -rf -- "$2"`)
	if err != nil {
		return err
	}
	if deleted.ReturnCode != 0 {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot remove verified collection source", nil)
	}
	return nil
}
