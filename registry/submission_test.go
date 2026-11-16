package registry

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSubmissionMigrationAndExplicitPrune(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "registry.db")
	reg, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id := reg.ID
	if _, err := reg.db.Exec("DROP TABLE submission_operations; UPDATE schema_meta SET value='2' WHERE key='schema_version'"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Close(); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	dir := filepath.Join(filepath.Dir(dbPath), "submissions", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	intent, result, nativeID := digest+".json", digest+".json.result", digest+".json.native-id"
	original := map[string][]byte{
		intent:   []byte(`{"Fingerprint":"old","Lookup":{"NativeID":""}}`),
		result:   []byte(`{"Result":{"outcome":"accepted"}}`),
		nativeID: []byte(`"42"`),
	}
	for name, body := range original {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	badName := filepath.Join(dir, "unknown")
	if err := os.WriteFile(badName, []byte("unidentified evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dbPath); err == nil {
		reopened.Close()
		t.Fatal("accepted unknown legacy evidence")
	}
	if err := os.Remove(badName); err != nil {
		t.Fatal(err)
	}
	reg, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	ctx := context.Background()
	body, legacy, err := reg.ReadSubmission(ctx, digest)
	var got map[string][]byte
	if err != nil || !legacy || json.Unmarshal(body, &got) != nil || !reflect.DeepEqual(got, original) || reg.ID != id {
		t.Fatalf("evidence=%v legacy=%v err=%v", got, legacy, err)
	}
	if claimed, err := reg.ClaimSubmission(ctx, digest, []byte("{}")); err != nil || claimed {
		t.Fatalf("legacy key can be claimed: %v %v", claimed, err)
	}
	if err := os.WriteFile(filepath.Join(dir, nativeID), []byte(`"43"`), 0o600); err != nil {
		t.Fatal(err)
	}
	removed, retained, err := reg.PruneImportedSubmissions(ctx)
	if err != nil || !reflect.DeepEqual(removed, []string{intent, result}) || !reflect.DeepEqual(retained, []string{nativeID}) {
		t.Fatalf("removed=%v retained=%v err=%v", removed, retained, err)
	}
	if claimed, err := reg.ClaimSubmission(ctx, digest, []byte("{}")); err != nil || claimed {
		t.Fatalf("pruned key can be claimed: %v %v", claimed, err)
	}
}
