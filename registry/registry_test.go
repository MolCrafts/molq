package registry

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/MolCrafts/molq/protocol"
	_ "modernc.org/sqlite"
)

func TestOpenPreservesIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := first.ID
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.ID != id || id == "" {
		t.Fatalf("registry id = %q then %q", id, second.ID)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}

func TestOpenRejectsForeignTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE jobs(id TEXT)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	_, err = Open(path)
	item, ok := err.(*protocol.Error)
	if !ok || item.Kind != "STATE_SCHEMA_INCOMPATIBLE" {
		t.Fatalf("err = %#v", err)
	}
}

func TestOpenRejectsIncompleteRegistryWithoutRepair(t *testing.T) {
	for _, damage := range []string{
		"DELETE FROM schema_meta WHERE key='registry_id'",
		"UPDATE schema_meta SET value='' WHERE key='registry_id'",
		"DROP TABLE clusters",
	} {
		t.Run(damage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "registry.db")
			reg, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reg.db.Exec(damage); err != nil {
				t.Fatal(err)
			}
			reg.Close()
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := Open(path)
			if opened != nil {
				opened.Close()
			}
			failure, ok := err.(*protocol.Error)
			if !ok || failure.Kind != "STATE_SCHEMA_INCOMPATIBLE" {
				t.Fatalf("error = %#v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("invalid registry was modified")
			}
		})
	}
}

func TestCorruptClusterDefinitionReturnsStructuredError(t *testing.T) {
	for _, definition := range []string{"null", "[]", "false", "invalid"} {
		t.Run(definition, func(t *testing.T) {
			reg, err := Open(filepath.Join(t.TempDir(), "registry.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer reg.Close()
			if _, err := reg.db.Exec("INSERT INTO clusters VALUES ('broken','broken',?,1)", definition); err != nil {
				t.Fatal(err)
			}
			_, err = reg.Get("broken")
			failure, ok := err.(*protocol.Error)
			if !ok || failure.Kind != "STATE_SCHEMA_INCOMPATIBLE" {
				t.Fatalf("error = %#v", err)
			}
			var stored string
			if err := reg.db.QueryRow("SELECT definition FROM clusters WHERE id='broken'").Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if stored != definition {
				t.Fatal("invalid definition was modified")
			}
		})
	}
}
