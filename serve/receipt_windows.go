package serve

import "os"

// Windows cannot fsync a directory through an ordinary file handle.
func syncDirectoryHandle(*os.File) error { return nil }
