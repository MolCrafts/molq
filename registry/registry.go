// Package registry stores destination identity. It does not store job history.
package registry

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/MolCrafts/molq/protocol"
	_ "modernc.org/sqlite"
)

const schemaVersion = "3"

const incompatibleRegistry = "Registry file is not a molq registry"

// Registry is the SQLite destination registry for one serve process.
type Registry struct {
	db   *sql.DB
	ID   string
	Path string
}

// DefaultPath is the registry used when the config file does not select one.
// It lives next to the configuration file.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = "."
	}
	return filepath.Join(home, ".molcrafts", "molq", "registry.db")
}

// Open creates or upgrades the serve-owned registry.
func Open(path string) (*Registry, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	db.SetMaxOpenConns(1)
	reg := &Registry{db: db, Path: path}
	if err := reg.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return reg, nil
}

func (r *Registry) initialize() error {
	if _, err := r.db.Exec("PRAGMA busy_timeout=100"); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	rows, err := r.db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	_ = rows.Close()
	owned := map[string]bool{"schema_meta": true, "clusters": true, "submission_operations": true}
	var upgrade bool
	extra := false
	for name := range tables {
		if !owned[name] {
			extra = true
		}
	}
	if extra || (len(tables) > 0 && (!tables["schema_meta"] || !tables["clusters"])) {
		return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", incompatibleRegistry, nil)
	}
	if tables["schema_meta"] {
		meta := map[string]string{}
		rows, err := r.db.Query("SELECT key, value FROM schema_meta")
		if err != nil {
			return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", incompatibleRegistry, nil)
		}
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				_ = rows.Close()
				return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
			}
			meta[key] = value
		}
		err = rows.Err()
		_ = rows.Close()
		upgrade = meta["schema_version"] == "2" && !tables["submission_operations"]
		current := meta["schema_version"] == schemaVersion && tables["submission_operations"]
		if err != nil || (!upgrade && !current) || meta["registry_id"] == "" {
			return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", incompatibleRegistry, nil)
		}
		rows, err = r.db.Query("SELECT id, name, definition, revision FROM clusters LIMIT 0")
		if err != nil {
			return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", incompatibleRegistry, nil)
		}
		_ = rows.Close()
		r.ID = meta["registry_id"]
	}
	if _, err := r.db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if _, err := r.db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if _, err := r.db.Exec("PRAGMA synchronous=FULL"); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot configure durable submission receipts", nil)
	}
	if upgrade {
		return r.migrateSubmissions()
	}
	if len(tables) > 0 {
		rows, err := r.db.Query("SELECT key_digest, record, final, legacy FROM submission_operations LIMIT 0")
		if err != nil {
			return protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", incompatibleRegistry, nil)
		}
		return rows.Close()
	}
	tx, err := r.db.Begin()
	if err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot initialize registry configuration", nil)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if _, err := tx.Exec(`CREATE TABLE clusters(id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, definition TEXT NOT NULL, revision INTEGER NOT NULL)`); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if _, err := tx.Exec(submissionTable); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot initialize submission storage", nil)
	}
	id, err := newUUID()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO schema_meta VALUES (?, ?)", "schema_version", schemaVersion); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if _, err := tx.Exec("INSERT INTO schema_meta VALUES (?, ?)", "registry_id", id); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	if err := tx.Commit(); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot open registry configuration", nil)
	}
	r.ID = id
	return nil
}

// Close releases the registry connection.
func (r *Registry) Close() error { return r.db.Close() }

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:]), nil
}

// NewHex is a UUID without hyphens, matching the serve process identity.
func NewHex() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x", value[:]), nil
}
