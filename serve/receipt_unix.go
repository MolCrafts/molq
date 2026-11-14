//go:build !windows

package serve

import "os"

func syncDirectoryHandle(dir *os.File) error { return dir.Sync() }
