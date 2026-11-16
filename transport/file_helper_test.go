package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

func fileIOInput(t *testing.T, request fileIORequest, data []byte) *bytes.Reader {
	t.Helper()
	header, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(append(append(header, '\n'), data...))
}

func TestPrivateFileHelperBoundsAndValidatesBeforeAccess(t *testing.T) {
	file := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(file, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		strings.Repeat("x", fileHelperHeaderLimit) + "\n",
		"{invalid}\n", `{"operation":"write","path":"` + file + `","data_size":4194305}` + "\n",
		`{"operation":"write","path":"` + file + `","data_size":3}` + "\nx",
		`{"operation":"remove","path":"` + file + `"}` + "\n",
	} {
		var output bytes.Buffer
		if err := FileIO(strings.NewReader(input), &output); err != nil {
			t.Fatal(err)
		}
		var response fileIOResponse
		if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Success || response.Failure == nil {
			t.Fatalf("malformed input response = %s, %v", &output, err)
		}
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "original" {
		t.Fatalf("invalid requests changed user data: %q, %v", data, err)
	}
}

func TestLocalFileHelperStagesScriptBeyondPublicChunkLimit(t *testing.T) {
	file := filepath.Join(t.TempDir(), "staged", "job.sh")
	data := bytes.Repeat([]byte("x"), 5*outputLimit)
	if err := (Local{}).Write(context.Background(), file, data, nil); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(actual, data) {
		t.Fatal("rendered script was truncated", err)
	}
	chunk, err := (Local{}).Read(context.Background(), file, fileChunkLimit, 1)
	if err != nil || string(chunk) != "x" {
		t.Fatalf("read chunk = %q, %v", chunk, err)
	}
}

func TestFileDeadlineReturnsBeforeReapAndKeepsAdmission(t *testing.T) {
	// A retained runner models Wait blocked on an uninterruptible syscall.
	// The fixture does not claim to create a live NFS kernel stall.
	fileHelperSlots <- struct{}{}
	started, reap := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := waitFileHelper(ctx, fileIORequest{Operation: "write", Path: "target"}, nil,
			func(*exec.Cmd) (fileIOResponse, error) {
				close(started)
				<-reap
				return fileIOResponse{Success: true}, nil
			})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		var failure *protocol.Error
		if !errors.As(err, &failure) || failure.Outcome != "unknown" {
			t.Fatalf("dispatched timeout = %#v", err)
		}
	case <-time.After(time.Second):
		close(reap)
		t.Fatal("file deadline waited for child reap")
	}
	if len(fileHelperSlots) != 1 {
		close(reap)
		t.Fatal("unreaped child released its admission permit")
	}
	for range fileHelperConcurrency - 1 {
		fileHelperSlots <- struct{}{}
	}
	queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := (Local{}).Write(queuedCtx, "not-dispatched", []byte("changed"), nil)
	queuedCancel()
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Outcome != "not_applied" || failure.Context["dispatch_attempted"] != false {
		t.Errorf("queued timeout = %#v", err)
	}
	for range fileHelperConcurrency - 1 {
		<-fileHelperSlots
	}
	close(reap)
	deadline := time.Now().Add(time.Second)
	for len(fileHelperSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(fileHelperSlots) != 0 {
		t.Fatal("reaped child retained its admission permit")
	}
}

func TestPrivateFileHelperReturnsOriginalErrors(t *testing.T) {
	var output bytes.Buffer
	zero := 0
	if err := FileIO(fileIOInput(t, fileIORequest{
		Operation: "read", Path: filepath.Join(t.TempDir(), "missing"), Offset: &zero, Limit: 1,
	}, nil), &output); err != nil {
		t.Fatal(err)
	}
	var response fileIOResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	_, err := decodeFileResponse(response, fileIORequest{Operation: "read"})
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Kind != "FILE_UNAVAILABLE" || failure.Outcome != "not_applied" {
		t.Fatalf("missing file response = %#v", err)
	}
}

func TestFileHelperCannotReportSuccessWithoutValidResponse(t *testing.T) {
	request := fileIORequest{Operation: "write"}
	for _, response := range []fileIOResponse{{}, {Success: true, Failure: map[string]any{"kind": "FILE_UNAVAILABLE"}}} {
		_, err := decodeFileResponse(response, request)
		var failure *protocol.Error
		if !errors.As(err, &failure) || failure.Outcome != "unknown" {
			t.Fatalf("incomplete helper response = %#v", err)
		}
	}
}

func TestFileHelperStartFailureProvesWriteWasNotApplied(t *testing.T) {
	fileHelperSlots <- struct{}{}
	cmd := exec.Command(filepath.Join(t.TempDir(), "missing-executable"))
	_, err := waitFileHelper(context.Background(), fileIORequest{Operation: "write"}, cmd, runFileHelper)
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Outcome != "not_applied" || failure.Context["dispatch_attempted"] != false {
		t.Fatalf("unstarted write helper = %#v", err)
	}
}

type chunkCheckingWriter struct {
	maximum int
	size    int
}

func (w *chunkCheckingWriter) Write(data []byte) (int, error) {
	if len(data) > w.maximum {
		return 0, errors.New("file helper exceeded its fixed streaming buffer")
	}
	w.size += len(data)
	return len(data), nil
}

func TestFileHelperStreamsExactlyDeclaredSizeInFixedChunks(t *testing.T) {
	writer := &chunkCheckingWriter{maximum: fileBlockSize}
	data := bytes.Repeat([]byte("x"), 3*fileBlockSize+17)
	n, err := copyFileContent(writer, bytes.NewReader(data), int64(len(data)-10))
	if err != nil || n != int64(len(data)-10) || writer.size != len(data)-10 {
		t.Fatalf("streamed n=%d, size=%d, error=%v", n, writer.size, err)
	}
}

func TestIncompleteOffsetFileStreamReportsUnknownMutation(t *testing.T) {
	file := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(file, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	zero := 0
	_, err := executeFileIO(fileIOInput(t, fileIORequest{
		Operation: "write", Path: file, Offset: &zero, DataSize: 3,
	}, []byte("ab")))
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Outcome != "unknown" {
		t.Fatalf("partial offset stream = %#v", err)
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "abiginal" {
		t.Fatalf("partial stream = %q, %v", data, err)
	}
}

func TestFileHelperCancellationKillsAndReapsOwnedChild(t *testing.T) {
	requirePOSIX(t)
	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", `printf started > "$1"; exec sleep 30`, "sh", marker)
	configureCommand(cmd)
	cmd.WaitDelay = time.Second
	fileHelperSlots <- struct{}{}
	done := make(chan error, 1)
	go func() {
		_, err := waitFileHelper(ctx, fileIORequest{Operation: "write"}, cmd, runFileHelper)
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned file helper did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		var failure *protocol.Error
		if !errors.As(err, &failure) || failure.Outcome != "unknown" {
			t.Fatalf("cancelled dispatched helper = %#v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owned helper ignored cancellation")
	}
	deadline = time.Now().Add(time.Second)
	for len(fileHelperSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(fileHelperSlots) != 0 {
		t.Fatal("owned helper was not reaped after cancellation")
	}
}
