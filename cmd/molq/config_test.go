package main

import (
	"github.com/MolCrafts/molq/config"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigUsesDefaultFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, ".molcrafts", "molq", "config.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"client":{"watch":{"interval":30,"clusters":["Arrhenius"]}}}`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	file, resolved, err := resolveConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != path || file.Client.Watch.Interval != 30 || len(file.Client.Watch.Clusters) != 1 || file.Client.Watch.Clusters[0] != "Arrhenius" {
		t.Fatalf("resolved=%s file=%+v", resolved, file.Client.Watch)
	}
}

func TestResolveConfigMissingDefaultIsEmpty(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	file, resolved, err := resolveConfig("")
	if err != nil || resolved != config.DefaultPath() || file.Client.Watch.Interval != 0 {
		t.Fatalf("resolved=%q err=%v interval=%v", resolved, err, file.Client.Watch.Interval)
	}
}

func TestResolveConfigInvalidDefaultErrors(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, ".molcrafts", "molq", "config.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := resolveConfig(""); err == nil {
		t.Fatal("expected invalid configuration to fail")
	}
}
