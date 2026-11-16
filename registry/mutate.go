package registry

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/MolCrafts/molq/protocol"
	"modernc.org/sqlite"
)

// List returns destination definitions without live job state.
func (r *Registry) List() ([]map[string]any, error) {
	rows, err := r.db.Query("SELECT id, definition, revision FROM clusters ORDER BY name")
	if err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
	}
	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		item, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, rows.Err()
}

// Get resolves one registered name or stable ID.
func (r *Registry) Get(key string) (map[string]any, error) {
	row := r.db.QueryRow("SELECT id, definition, revision FROM clusters WHERE id=? OR name=?", key, key)
	var id, definition string
	var revision int
	if err := row.Scan(&id, &definition, &revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, protocol.Fail("CLUSTER_NOT_FOUND", "Unknown Cluster", map[string]any{"cluster": key})
		}
		return nil, protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
	}
	return decodeCluster(id, definition, revision)
}

// Mutate registers, updates, or removes one destination under a revision check.
func (r *Registry) Mutate(definition map[string]any, key string, expectedRevision *int) (map[string]any, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	var identity string
	if key == "" {
		identity, err = newUUID()
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(definition)
		if err != nil {
			return nil, protocol.Fail("INVALID_INPUT", "Cluster definition is not JSON", nil)
		}
		if _, err := tx.Exec("INSERT INTO clusters VALUES (?,?,?,1)", identity, definition["name"], string(encoded)); err != nil {
			return nil, registryWriteError(err)
		}
	} else {
		old, err := getTx(tx, key)
		if err != nil {
			return nil, err
		}
		identity, _ = old["id"].(string)
		revision, _ := asInt(old["revision"])
		if expectedRevision == nil || revision != *expectedRevision {
			return nil, protocol.Fail("CONFLICT", "Cluster revision changed", map[string]any{"current_revision": revision})
		}
		if definition == nil {
			if _, err := tx.Exec("DELETE FROM clusters WHERE id=?", identity); err != nil {
				return nil, registryWriteError(err)
			}
		} else {
			for _, field := range []string{"scheduler", "transport"} {
				if !jsonEqual(old[field], definition[field]) {
					return nil, protocol.Fail("INVALID_INPUT", "Changed compute destination requires a new Cluster ID", map[string]any{"field": field})
				}
			}
			encoded, err := json.Marshal(definition)
			if err != nil {
				return nil, protocol.Fail("INVALID_INPUT", "Cluster definition is not JSON", nil)
			}
			if _, err := tx.Exec("UPDATE clusters SET name=?, definition=?, revision=revision+1 WHERE id=?", definition["name"], string(encoded), identity); err != nil {
				return nil, registryWriteError(err)
			}
		}
	}
	var result map[string]any
	if definition != nil {
		result, err = getTx(tx, identity)
		if err != nil {
			return nil, err
		}
	} else {
		result = map[string]any{"removed": identity}
	}
	if err := tx.Commit(); err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
	}
	rollback = false
	return result, nil
}

func getTx(tx *sql.Tx, key string) (map[string]any, error) {
	var id, definition string
	var revision int
	err := tx.QueryRow("SELECT id, definition, revision FROM clusters WHERE id=? OR name=?", key, key).Scan(&id, &definition, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocol.Fail("CLUSTER_NOT_FOUND", "Unknown Cluster", map[string]any{"cluster": key})
	}
	if err != nil {
		return nil, protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
	}
	return decodeCluster(id, definition, revision)
}

func scanCluster(rows *sql.Rows) (map[string]any, error) {
	var id, definition string
	var revision int
	if err := rows.Scan(&id, &definition, &revision); err != nil {
		return nil, err
	}
	return decodeCluster(id, definition, revision)
}

func decodeCluster(id, definition string, revision int) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal([]byte(definition), &body); err != nil || body == nil {
		return nil, protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", "Cluster definition is not an object", nil)
	}
	body["id"] = id
	body["revision"] = revision
	return body, nil
}

func registryWriteError(err error) error {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code() == 19 {
		return protocol.Fail("CONFLICT", "Cluster name already registered", nil)
	}
	return protocol.Fail("STATE_BUSY", "Registry busy; retry this configuration operation", nil)
}

func jsonEqual(left, right any) bool {
	a, _ := json.Marshal(left)
	b, _ := json.Marshal(right)
	return string(a) == string(b)
}

func asInt(value any) (int, bool) {
	switch item := value.(type) {
	case int:
		return item, true
	case int64:
		return int(item), true
	case float64:
		return int(item), true
	default:
		return 0, false
	}
}
