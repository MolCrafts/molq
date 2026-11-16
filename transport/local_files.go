package transport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"github.com/MolCrafts/molq/protocol"
	"io"
	"os"
	"path/filepath"
)

func readLocalFile(ctx context.Context, path string, offset, limit int) ([]byte, error) {
	if offset < 0 || limit < 0 || limit > fileChunkLimit {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid target file range", nil)
	}
	if err := regularPath(ctx, path, false); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|nonBlockingFileFlag(), 0)
	if err != nil {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read target file", map[string]any{"path": path})
	}
	defer file.Close()
	if err := regularHandle(ctx, file, path); err != nil {
		return nil, err
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read target file", map[string]any{"path": path})
	}
	if err := fileContext(ctx, path); err != nil {
		return nil, err
	}
	buf := make([]byte, limit)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read target file", map[string]any{"path": path})
	}
	if err := fileContext(ctx, path); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func writeLocalStream(ctx context.Context, path string, input io.Reader, size int64, offset *int) error {
	if size < 0 || offset != nil && *offset < 0 {
		return protocol.Fail("INVALID_INPUT", "Invalid target file size or offset", nil)
	}
	if err := regularPath(ctx, path, true); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot write target file", map[string]any{"path": path})
	}
	if offset == nil {
		return atomicLocalWrite(ctx, path, input, size)
	}
	return offsetLocalWrite(ctx, path, input, size, *offset)
}

func atomicLocalWrite(ctx context.Context, path string, input io.Reader, size int64) error {
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	temp := path + "." + hex.EncodeToString(nonce[:]) + ".tmp"
	defer os.Remove(temp)
	if err := fileContext(ctx, path); err != nil {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot write target file", map[string]any{"path": path})
	}
	_, copyErr := copyFileContent(file, input, size)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot write complete target file", map[string]any{"path": path})
	}
	if err := regularPath(ctx, path, true); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return protocol.FailOutcome("FILE_UNAVAILABLE", "Target file replacement failed", "unknown", map[string]any{"path": path})
	}
	return nil
}

func offsetLocalWrite(ctx context.Context, path string, input io.Reader, size int64, offset int) error {
	flags := os.O_RDWR | os.O_CREATE | nonBlockingFileFlag()
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot write target file", map[string]any{"path": path})
	}
	defer file.Close()
	if err := regularHandle(ctx, file, path); err != nil {
		return err
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Cannot write target file", map[string]any{"path": path})
	}
	if err := fileContext(ctx, path); err != nil {
		return err
	}
	if n, err := copyFileContent(file, input, size); err != nil {
		outcome := "not_applied"
		if n > 0 {
			outcome = "unknown"
		}
		return protocol.FailOutcome("FILE_UNAVAILABLE", "Cannot write complete target file", outcome, map[string]any{"path": path})
	}
	return nil
}

func copyFileContent(file io.Writer, input io.Reader, size int64) (int64, error) {
	// Hide optional ReaderFrom/WriterTo so this helper always uses a fixed chunk.
	n, err := io.CopyBuffer(struct{ io.Writer }{file}, io.LimitReader(input, size), make([]byte, fileBlockSize))
	if err == nil && n != size {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func fileContext(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		return protocol.Fail("FILE_UNAVAILABLE", "Target file operation cancelled", map[string]any{"path": path})
	}
	return nil
}

// Regular files support the bounded, seekable chunks promised by file RPCs.
// Nonblocking opens also protect against a path becoming a FIFO after Stat.
func regularPath(ctx context.Context, path string, allowMissing bool) error {
	if err := fileContext(ctx, path); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if allowMissing && os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return protocol.Fail("FILE_UNAVAILABLE", "Target path must be a regular file", map[string]any{"path": path})
	}
	return fileContext(ctx, path)
}

func regularHandle(ctx context.Context, file *os.File, path string) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return protocol.Fail("FILE_UNAVAILABLE", "Target path must be a regular file", map[string]any{"path": path})
	}
	return fileContext(ctx, path)
}
