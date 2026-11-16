//go:build !windows

package serve

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/MolCrafts/molq/transport"
)

// uutils coreutils (Ubuntu 25.10+) parse `date -r -name` as an option, unlike
// GNU date, so names with a leading dash must still yield metadata.
func TestDashPrefixedNamesKeepTheirMetadata(t *testing.T) {
	base := t.TempDir()
	service := openTransferService(t, base)
	root := filepath.Join(base, "scope")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "-dash"), []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := service.Call("files.list", map[string]any{
		"cluster": "from", "path": root, "root": root, "with_stat": true,
	})
	if err != nil {
		t.Fatalf("files.list: %v", err)
	}
	entries := result.(map[string]any)["entries"].([]map[string]any)
	if len(entries) != 1 || entries[0]["name"] != "-dash" || entries[0]["size"] != int64(5) || entries[0]["mtime"].(int64) <= 0 {
		t.Fatalf("entries = %#v", entries)
	}

	t.Chdir(root)
	output, err := runFileScript(context.Background(), transport.Local{}, false, "-dash", statScript, "-dash")
	if err != nil {
		t.Fatalf("stat of a relative dash name: %v", err)
	}
	stat, err := parseStat("-dash", output)
	if err != nil || stat.(map[string]any)["size"] != int64(5) {
		t.Fatalf("stat = %#v, %v", stat, err)
	}
}
