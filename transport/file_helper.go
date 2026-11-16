package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

const fileHelperTimeout = 30 * time.Second
const fileHelperHeaderLimit = 65536
const fileHelperConcurrency = 8

// Admission remains occupied until the owned child is reaped, even if the
// kernel delays SIGKILL during filesystem I/O and the caller has already left.
var fileHelperSlots = make(chan struct{}, fileHelperConcurrency)

type fileIORequest struct {
	Operation string `json:"operation"`
	Path      string `json:"path"`
	Offset    *int   `json:"offset,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	DataSize  int    `json:"data_size,omitempty"`
}

type fileIOResponse struct {
	Success bool           `json:"success"`
	Data    []byte         `json:"data,omitempty"`
	Failure map[string]any `json:"error,omitempty"`
}

// FileIO executes one private bounded file request for serve --file-io.
// It is called explicitly by the process entry point, before service setup.
// Target filesystem syscalls occur only in this disposable owned child.
func FileIO(input io.Reader, output io.Writer) error {
	data, err := executeFileIO(input)
	response := fileIOResponse{Success: err == nil, Data: data}
	if err != nil {
		var failure *protocol.Error
		if !errors.As(err, &failure) {
			failure = protocol.Fail("FILE_UNAVAILABLE", "Target file helper failed", nil)
		}
		response.Failure = failure.Data()
	}
	return json.NewEncoder(output).Encode(response)
}

func executeFileIO(input io.Reader) ([]byte, error) {
	reader := bufio.NewReaderSize(input, fileHelperHeaderLimit)
	header, err := reader.ReadSlice('\n')
	if err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid private file request header", nil)
	}
	var request fileIORequest
	if err := json.Unmarshal(header, &request); err != nil || request.Path == "" {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid private file request", nil)
	}
	if request.Operation == "read" && request.Offset != nil && request.DataSize == 0 {
		return readLocalFile(context.Background(), request.Path, *request.Offset, request.Limit)
	}
	if request.Operation != "write" || request.DataSize < 0 {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid private file operation or size", nil)
	}
	return nil, writeLocalStream(context.Background(), request.Path, reader, int64(request.DataSize), request.Offset)
}

func (Local) Read(ctx context.Context, path string, offset, limit int) ([]byte, error) {
	if offset < 0 || limit < 0 || limit > fileChunkLimit {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid target file range", nil)
	}
	return localFileRequest(ctx, fileIORequest{Operation: "read", Path: path, Offset: &offset, Limit: limit}, nil)
}

func (Local) Write(ctx context.Context, path string, data []byte, offset *int) error {
	if offset != nil && *offset < 0 {
		return protocol.Fail("INVALID_INPUT", "Invalid target file offset or size", nil)
	}
	_, err := localFileRequest(ctx, fileIORequest{Operation: "write", Path: path, Offset: offset, DataSize: len(data)}, data)
	return err
}

func localFileRequest(ctx context.Context, request fileIORequest, data []byte) ([]byte, error) {
	runCtx, cancel := context.WithTimeout(ctx, fileHelperTimeout)
	defer cancel()
	if runCtx.Err() != nil {
		return nil, fileHelperFailure(request, false, "Target file operation cancelled before dispatch")
	}
	select {
	case fileHelperSlots <- struct{}{}:
	case <-runCtx.Done():
		return nil, fileHelperFailure(request, false, "Target file operation cancelled before dispatch")
	}
	if runCtx.Err() != nil {
		<-fileHelperSlots
		return nil, fileHelperFailure(request, false, "Target file operation cancelled before dispatch")
	}
	return dispatchFileHelper(runCtx, request, data)
}

func fileHelperFailure(request fileIORequest, attempted bool, message string) error {
	context := map[string]any{"path": request.Path, "dispatch_attempted": attempted}
	if request.Operation == "write" && attempted {
		return protocol.FailOutcome("OUTCOME_UNKNOWN", message, "unknown", context)
	}
	return protocol.Fail("FILE_UNAVAILABLE", message, context)
}

type fileHelperResult struct {
	response  fileIOResponse
	err       error
	attempted bool
}

func dispatchFileHelper(ctx context.Context, request fileIORequest, data []byte) ([]byte, error) {
	executable, err := os.Executable()
	if err != nil {
		<-fileHelperSlots
		return nil, fileHelperFailure(request, false, "Cannot locate target file helper")
	}
	header, _ := json.Marshal(request)
	if len(header)+1 > fileHelperHeaderLimit {
		<-fileHelperSlots
		return nil, protocol.Fail("INVALID_INPUT", "Target file request header is too large", nil)
	}
	// The child may outlive the caller during uninterruptible kernel I/O.
	// Keep its input private even if the caller reuses its write buffer.
	input := io.MultiReader(bytes.NewReader(append(header, '\n')), bytes.NewReader(bytes.Clone(data)))
	cmd := exec.CommandContext(ctx, executable, "serve", "--file-io")
	configureCommand(cmd)
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = input
	return waitFileHelper(ctx, request, cmd, runFileHelper)
}

func waitFileHelper(ctx context.Context, request fileIORequest, cmd *exec.Cmd, execute func(*exec.Cmd) (fileIOResponse, error)) ([]byte, error) {
	done := make(chan fileHelperResult, 1)
	var attempted atomic.Bool
	go func() {
		defer func() { <-fileHelperSlots }()
		if ctx.Err() != nil {
			done <- fileHelperResult{err: ctx.Err()}
			return
		}
		attempted.Store(true)
		response, err := execute(cmd)
		// A completed Start failure proves that no child could mutate the file.
		// While Start is still blocked, the timeout path remains conservative.
		started := cmd == nil || cmd.Process != nil
		done <- fileHelperResult{response: response, err: err, attempted: started}
	}()
	select {
	case <-ctx.Done():
		return nil, fileHelperFailure(request, attempted.Load(), "Target file operation deadline or cancellation reached")
	case result := <-done:
		if result.err != nil {
			return nil, fileHelperFailure(request, result.attempted, "Target file helper failed or disconnected")
		}
		return decodeFileResponse(result.response, request)
	}
}

func runFileHelper(cmd *exec.Cmd) (fileIOResponse, error) {
	var stdout, stderr bytes.Buffer
	var exceeded atomic.Bool
	cmd.Stdout = limitWriter{buf: &stdout, exceeded: &exceeded, cancel: func() { _ = cmd.Cancel() }}
	cmd.Stderr = limitWriter{buf: &stderr, exceeded: &exceeded, cancel: func() { _ = cmd.Cancel() }}
	if err := cmd.Run(); err != nil {
		return fileIOResponse{}, err
	}
	var response fileIOResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		return fileIOResponse{}, err
	}
	return response, nil
}

func decodeFileResponse(response fileIOResponse, request fileIORequest) ([]byte, error) {
	if response.Failure != nil {
		failure := response.Failure
		kind, _ := failure["kind"].(string)
		message, _ := failure["message"].(string)
		outcome, _ := failure["outcome"].(string)
		context, _ := failure["context"].(map[string]any)
		if kind != "" && message != "" && (outcome == "not_applied" || outcome == "unknown") {
			return nil, protocol.FailOutcome(kind, message, outcome, context)
		}
	}
	if response.Failure != nil || !response.Success || request.Operation == "read" && len(response.Data) > request.Limit {
		return nil, fileHelperFailure(request, true, "Invalid target file helper response")
	}
	return response.Data, nil
}
