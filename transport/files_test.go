package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFilesCancelledBeforeAccess(t *testing.T) {
	file := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(file, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Local{}).Read(ctx, file, 0, 8); err == nil {
		t.Error("cancelled read succeeded")
	}
	for _, offset := range []*int{nil, new(int)} {
		if err := (Local{}).Write(ctx, file, []byte("changed"), offset); err == nil {
			t.Error("cancelled write succeeded")
		}
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "original" {
		t.Fatalf("cancelled writes changed user data: %q, %v", data, err)
	}
}

func TestLocalFilesRegularSymlinkSemantics(t *testing.T) {
	requirePOSIX(t)
	root := t.TempDir()
	file, link := filepath.Join(root, "data"), filepath.Join(root, "link")
	if err := os.WriteFile(file, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	data, err := (Local{}).Read(context.Background(), link, 1, 3)
	if err != nil || string(data) != "rig" {
		t.Fatalf("read through symlink = %q, %v", data, err)
	}
	offset := 1
	if err := (Local{}).Write(context.Background(), link, []byte("123"), &offset); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Readlink(link); err != nil {
		t.Fatal("offset write removed symlink", err)
	}
	data, err = os.ReadFile(file)
	if err != nil || string(data) != "o123inal" {
		t.Fatalf("offset write through symlink = %q, %v", data, err)
	}
	if err := (Local{}).Write(context.Background(), link, []byte("replacement"), nil); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(file)
	if err != nil || string(data) != "o123inal" {
		t.Fatalf("atomic replacement changed symlink target: %q, %v", data, err)
	}
}
