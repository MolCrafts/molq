package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"gopkg.in/yaml.v3"
)

var settingKinds = map[string]string{
	"default_cluster":   "string",
	"cache_max_age":     "number",
	"cache_max_entries": "int",
}

// Store is the serve-owned configuration file. client and serve sections are
// preserved and are not scheduling state.
type Store struct {
	mu   sync.RWMutex
	path string
	data map[string]any
}

// Open loads one JSON or YAML file. A missing file starts empty.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, protocol.Fail("INVALID_INPUT", "Configuration file is required", nil)
	}
	if ext := filepath.Ext(path); ext != ".json" && ext != ".yaml" && ext != ".yml" {
		return nil, protocol.Fail("INVALID_INPUT", "Configuration must be JSON or YAML", nil)
	}
	store := &Store{path: path}
	if err := store.Reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// Reload replaces the snapshot after the file validates.
func (s *Store) Reload() error {
	return s.locked(func() error {
		data, err := s.read()
		if err != nil {
			return err
		}
		s.data = data
		return nil
	})
}

// Get returns one setting or preset. A missing key has a null value.
func (s *Store) Get(namespace, key string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := knownNamespace(namespace); err != nil {
		return nil, err
	}
	if namespace == "settings" {
		if _, ok := settingKinds[key]; !ok {
			return nil, protocol.Fail("INVALID_INPUT", "Unknown runtime setting", map[string]any{"key": key})
		}
	}
	return map[string]any{"key": key, "value": clone(s.section(namespace)[key])}, nil
}

// List returns one namespace in key order.
func (s *Store) List(namespace string) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := knownNamespace(namespace); err != nil {
		return nil, err
	}
	section := s.section(namespace)
	keys := make([]string, 0, len(section))
	for key := range section {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{"key": key, "value": clone(section[key])})
	}
	return out, nil
}

// Set writes one validated key and returns the stored value.
func (s *Store) Set(namespace, key string, value any) (map[string]any, error) {
	if err := knownNamespace(namespace); err != nil {
		return nil, err
	}
	if err := s.locked(func() error {
		data, err := s.read()
		if err != nil {
			return err
		}
		section, _ := data[namespace].(map[string]any)
		section[key] = clone(value)
		data[namespace] = section
		return s.save(data)
	}); err != nil {
		return nil, err
	}
	return map[string]any{"key": key, "value": clone(value)}, nil
}

// Entries reads one integer setting, or fallback when it is absent.
func (s *Store) Entries(fallback int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := whole(s.section("settings")["cache_max_entries"])
	if !ok || value <= 0 {
		return fallback
	}
	return value
}

// MaxAge reads cache_max_age seconds, or fallback when it is absent.
func (s *Store) MaxAge(fallback float64) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := number(s.section("settings")["cache_max_age"])
	if !ok || value <= 0 {
		return fallback
	}
	return value
}

func (s *Store) section(namespace string) map[string]any {
	item, _ := s.data[namespace].(map[string]any)
	if item == nil {
		return map[string]any{}
	}
	return item
}

func (s *Store) read() (map[string]any, error) {
	if _, err := os.Stat(s.path); err != nil {
		if os.IsNotExist(err) {
			return emptyDocument(), nil
		}
		return nil, protocol.Fail("INVALID_INPUT", "Cannot load configuration", map[string]any{"path": s.path})
	}
	body, err := os.ReadFile(s.path)
	if err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Cannot load configuration", map[string]any{"path": s.path})
	}
	parsed := map[string]any{}
	if len(strings.TrimSpace(string(body))) == 0 {
		return emptyDocument(), nil
	}
	if strings.HasSuffix(s.path, ".yaml") || strings.HasSuffix(s.path, ".yml") {
		err = yaml.Unmarshal(body, &parsed)
	} else {
		err = json.Unmarshal(body, &parsed)
	}
	if err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Cannot load configuration", map[string]any{"path": s.path})
	}
	return validateDocument(parsed)
}

func (s *Store) save(data map[string]any) error {
	validated, err := validateDocument(data)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	var encoded []byte
	if strings.HasSuffix(s.path, ".yaml") || strings.HasSuffix(s.path, ".yml") {
		encoded, err = yaml.Marshal(validated)
	} else {
		encoded, err = json.MarshalIndent(validated, "", "  ")
	}
	if err != nil {
		return protocol.Fail("INVALID_INPUT", "Cannot write configuration", nil)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".molq-config-")
	if err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err := temp.Write(append(encoded, '\n')); err != nil {
		_ = temp.Close()
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	if err := temp.Close(); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	if err := os.Rename(name, s.path); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot write configuration", nil)
	}
	s.data = validated
	return nil
}

func (s *Store) locked(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot lock configuration", nil)
	}
	lock, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return protocol.Fail("STATE_BUSY", "Cannot lock configuration", nil)
	}
	defer lock.Close()
	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		err = lockFile(lock)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return protocol.Fail("STATE_BUSY", "Cannot lock configuration", nil)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer unlockFile(lock)
	return fn()
}

func validateDocument(data map[string]any) (map[string]any, error) {
	for key := range data {
		switch key {
		case "settings", "presets", "client", "serve":
		default:
			return nil, protocol.Fail("INVALID_INPUT", "Unknown configuration namespace", nil)
		}
	}
	out := emptyDocument()
	for _, namespace := range []string{"settings", "presets", "client", "serve"} {
		if data[namespace] == nil {
			continue
		}
		section, ok := data[namespace].(map[string]any)
		if !ok {
			return nil, protocol.Fail("INVALID_INPUT", "Configuration namespaces must be objects", nil)
		}
		copied := map[string]any{}
		for key, value := range section {
			copied[key] = value
		}
		out[namespace] = copied
	}
	settings := out["settings"].(map[string]any)
	for key, value := range settings {
		kind, ok := settingKinds[key]
		if !ok || !validSetting(kind, value) {
			return nil, protocol.Fail("INVALID_INPUT", "Unknown or invalid runtime setting", map[string]any{"key": key})
		}
	}
	presets := out["presets"].(map[string]any)
	for key, value := range presets {
		if err := protocol.Current().ValidateDocument(map[string]any{"key": key, "value": value}, "#/methods/presets.set"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func validSetting(kind string, value any) bool {
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "int":
		number, ok := whole(value)
		return ok && number > 0 && number <= 1000000
	case "number":
		number, ok := number(value)
		return ok && number > 0 && number <= 1000000
	default:
		return false
	}
}

func whole(value any) (int, bool) {
	switch item := value.(type) {
	case int:
		return item, true
	case int64:
		return int(item), true
	case float64:
		if item != float64(int(item)) {
			return 0, false
		}
		return int(item), true
	case bool:
		return 0, false
	default:
		return 0, false
	}
}

func number(value any) (float64, bool) {
	switch item := value.(type) {
	case int:
		return float64(item), true
	case int64:
		return float64(item), true
	case float64:
		return item, true
	case bool:
		return 0, false
	default:
		return 0, false
	}
}

func emptyDocument() map[string]any {
	return map[string]any{
		"settings": map[string]any{},
		"presets":  map[string]any{},
		"client":   map[string]any{},
		"serve":    map[string]any{},
	}
}

func knownNamespace(namespace string) error {
	if namespace != "settings" && namespace != "presets" {
		return protocol.Fail("INVALID_INPUT", "Invalid configuration namespace", nil)
	}
	return nil
}

func clone(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if json.Unmarshal(encoded, &out) != nil {
		return value
	}
	return out
}
