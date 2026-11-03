// Package rpc frames stdio and HTTP calls onto the serve process.
package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/serve"
)

const maxFrame = 1048576

var mutations = map[string]bool{
	"clusters.register": true,
	"clusters.update":   true,
	"clusters.remove":   true,
	"jobs.submit":       true,
	"jobs.cancel":       true,
	"jobs.cancel_many":  true,
	"jobs.purge":        true,
	"files.transfer":    true,
	"files.write":       true,
	"files.mkdir":       true,
	"files.remove":      true,
	"files.rename":      true,
	"files.touch":       true,
	"config.set":        true,
	"config.reload":     true,
	"presets.set":       true,
}

// Dispatch validates one envelope or a batch and executes it.
func Dispatch(service *serve.Service, payload any, streaming bool) any {
	batch, ok := payload.([]any)
	if ok {
		if len(batch) == 0 || len(batch) > 128 {
			return errorResponse(nil, protocol.Fail("INVALID_INPUT", "Batch size must be 1..128", nil), -32600)
		}
		responses := make([]any, 0, len(batch))
		for _, item := range batch {
			if response := Dispatch(service, item, streaming); response != nil {
				responses = append(responses, response)
			}
		}
		if len(responses) == 0 {
			return nil
		}
		return bounded(responses)
	}
	request, ok := payload.(map[string]any)
	if !ok || request["jsonrpc"] != "2.0" || !isString(request["method"]) || !validID(request) {
		return errorResponse(nil, protocol.Fail("INVALID_INPUT", "Invalid JSON-RPC envelope", nil), -32600)
	}
	method := request["method"].(string)
	_, hasID := request["id"]
	if !hasID && mutations[method] {
		return nil
	}
	params, present := request["params"]
	if !present {
		params = map[string]any{}
	}
	object, ok := params.(map[string]any)
	if !ok {
		if !hasID {
			return nil
		}
		return errorResponse(request["id"], protocol.Fail("INVALID_INPUT", "Named RPC params must be an object", nil), -32602)
	}
	if method == "events.subscribe" && !streaming {
		if !hasID {
			return nil
		}
		return errorResponse(request["id"], protocol.Fail("RPC_CHANNEL_UNSUPPORTED", "Subscriptions require stdio or WebSocket", nil), -32000)
	}
	result, err := service.Call(method, object)
	if err != nil {
		if !hasID {
			return nil
		}
		return errorResponse(request["id"], asError(err), codeFor(err))
	}
	if !hasID {
		return nil
	}
	return bounded(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": result})
}

// Stdio serves newline-delimited frames until EOF.
func Stdio(service *serve.Service, in io.Reader, out io.Writer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stop func()
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	var writeMu sync.Mutex
	write := func(value any) error {
		encoded, err := marshal(value)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, err = out.Write(append(encoded, '\n'))
		return err
	}
	reader := bufReader{r: bufio.NewReader(in)}
	for {
		frame, err := reader.readLine(maxFrame + 1)
		if err == io.EOF && len(frame) == 0 {
			return nil
		}
		if err != nil && err != io.EOF {
			return err
		}
		payload, decodeErr := decode(frame)
		var response any
		if decodeErr != nil {
			response = errorResponse(nil, asError(decodeErr), -32700)
		} else {
			response = Dispatch(service, payload, true)
		}
		if response != nil {
			if err := write(response); err != nil {
				return err
			}
			if next := startEvents(ctx, service, response, payload, write); next != nil {
				if stop != nil {
					stop()
				}
				stop = next
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

func startEvents(ctx context.Context, service *serve.Service, response, payload any, write func(any) error) func() {
	request, ok := payload.(map[string]any)
	if !ok || request["method"] != "events.subscribe" {
		return nil
	}
	body, ok := response.(map[string]any)
	if !ok || body["result"] == nil {
		return nil
	}
	result, _ := body["result"].(map[string]any)
	cursor, _ := result["cursor"].(string)
	updates, stop := service.Listen()
	streamCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer stop()
		defer cancel()
		streamSocket(streamCtx, service, cursor, updates, write)
	}()
	return func() { cancel(); stop() }
}

func decode(frame []byte) (any, error) {
	if len(frame) > maxFrame {
		return nil, protocol.Fail("INVALID_INPUT", "RPC frame exceeds 1 MiB", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Malformed JSON frame", nil)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, protocol.Fail("INVALID_INPUT", "Malformed JSON frame", nil)
	}
	return normalize(payload), nil
}

func normalize(value any) any {
	switch item := value.(type) {
	case json.Number:
		if integer, err := item.Int64(); err == nil {
			return integer
		}
		float, err := item.Float64()
		if err != nil {
			return item.String()
		}
		return float
	case map[string]any:
		for key, child := range item {
			item[key] = normalize(child)
		}
		return item
	case []any:
		for i, child := range item {
			item[i] = normalize(child)
		}
		return item
	default:
		return value
	}
}

func errorResponse(id any, err *protocol.Error, code int) map[string]any {
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": err.Error(), "data": err.Data()},
	}
}

func asError(err error) *protocol.Error {
	if item, ok := err.(*protocol.Error); ok {
		return item
	}
	return &protocol.Error{
		Kind: "INTERNAL_ERROR", Message: "Serve operation failed; consult serve diagnostics",
		Outcome: "unknown", Context: map[string]any{},
	}
}

func codeFor(err error) int {
	item, ok := err.(*protocol.Error)
	if !ok {
		return -32000
	}
	switch item.Kind {
	case "METHOD_NOT_FOUND":
		return -32601
	case "INVALID_INPUT":
		return -32602
	default:
		return -32000
	}
}

func bounded(value any) any {
	encoded, err := marshal(value)
	if err != nil || len(encoded) > maxFrame {
		id := any(nil)
		if item, ok := value.(map[string]any); ok {
			id = item["id"]
		}
		return errorResponse(id, protocol.Fail("PARTIAL_FAILURE", "RPC response exceeds 1 MiB; narrow the query", map[string]any{"outcome": "unknown"}), -32000)
	}
	return value
}

func marshal(value any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func isString(value any) bool {
	_, ok := value.(string)
	return ok
}

func validID(request map[string]any) bool {
	id, ok := request["id"]
	if !ok {
		return true
	}
	switch id.(type) {
	case string, int, int64, nil:
		return true
	default:
		return false
	}
}

type bufReader struct{ r *bufio.Reader }

func (b bufReader) readLine(limit int) ([]byte, error) {
	frame := make([]byte, 0, 4096)
	for {
		chunk, err := b.r.ReadSlice('\n')
		if err == nil {
			chunk = chunk[:len(chunk)-1]
		}
		remaining := limit - len(frame)
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		frame = append(frame, chunk...)
		// Keep draining an oversized frame to LF; its suffix is never a request.
		if err != bufio.ErrBufferFull {
			return frame, err
		}
	}
}
