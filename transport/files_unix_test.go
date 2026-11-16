//go:build !windows

package transport

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLocalFilesRejectSpecialFilesAndPreserveThem(t *testing.T) {
	file := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(file, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (Local{}).Write(context.Background(), file, []byte("replacement"), nil); err == nil {
		t.Error("atomic write replaced a FIFO")
	}
	info, err := os.Stat(file)
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO was changed: %v, %v", info, err)
	}
	if _, err := (Local{}).Read(context.Background(), "/dev/null", 0, 1); err == nil {
		t.Error("device read succeeded")
	}
	zero := 0
	if err := (Local{}).Write(context.Background(), "/dev/null", []byte("x"), &zero); err == nil {
		t.Error("device write succeeded")
	}
}
