package config

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestSetPreservesClientAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"client":{"executable":"/usr/local/bin/molq"},"settings":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("settings", "cache_max_entries", 12); err != nil {
		t.Fatal(err)
	}
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get("settings", "cache_max_entries")
	if err != nil {
		t.Fatal(err)
	}
	value, ok := whole(got["value"])
	if !ok || value != 12 {
		t.Fatalf("entries = %#v", got["value"])
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "/usr/local/bin/molq") {
		t.Fatalf("client section dropped: %s", body)
	}
}

func TestRejectsUnknownSetting(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("settings", "cadence", 1); err == nil {
		t.Fatal("expected unknown setting")
	}
}

func TestConcurrentStoreSnapshots(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls sync.WaitGroup
	for range 4 {
		calls.Go(func() {
			for range 100 {
				if _, err := store.Get("settings", "cache_max_entries"); err != nil {
					t.Error(err)
				}
				if _, err := store.List("settings"); err != nil {
					t.Error(err)
				}
				store.Entries(4096)
				store.MaxAge(5)
			}
		})
	}
	calls.Go(func() {
		for i := 1; i <= 20; i++ {
			if _, err := store.Set("settings", "cache_max_entries", i); err != nil {
				t.Error(err)
			}
			if err := store.Reload(); err != nil {
				t.Error(err)
			}
		}
	})
	calls.Wait()
}

func TestBusyStorePreservesSettings(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set("settings", "cache_max_entries", 12); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(store.path+".lock", os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := lockFile(lock); err != nil {
		t.Fatal(err)
	}
	defer unlockFile(lock)
	_, err = store.Set("settings", "cache_max_entries", 99)
	failure, ok := err.(*protocol.Error)
	if !ok || failure.Kind != "STATE_BUSY" {
		t.Fatalf("failure = %#v", err)
	}
	after, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || store.Entries(0) != 12 {
		t.Fatal("busy mutation changed settings")
	}
}
