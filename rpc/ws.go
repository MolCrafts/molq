package rpc

import (
	"context"
	"crypto/subtle"
	"net/http"
	"sync"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/serve"
	"github.com/coder/websocket"
)

func acceptSocket(w http.ResponseWriter, r *http.Request, service *serve.Service, token string) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "")
	conn.SetReadLimit(maxFrame)
	authCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	_, message, err := conn.Read(authCtx)
	cancel()
	if err != nil {
		conn.Close(websocket.StatusPolicyViolation, "")
		return
	}
	payload, err := decode(message)
	auth, _ := payload.(map[string]any)
	supplied, _ := auth["token"].(string)
	if err != nil || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
		_ = writeSocket(r.Context(), conn, errorResponse(nil, protocol.Fail("AUTH_REQUIRED", "First WS frame must authenticate", nil), -32000))
		conn.Close(websocket.StatusPolicyViolation, "")
		return
	}
	var stop func()
	var writeMu sync.Mutex
	write := func(value any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeSocket(r.Context(), conn, value)
	}
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	for {
		_, message, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		payload, err := decode(message)
		if err != nil {
			_ = write(errorResponse(nil, asError(err), -32700))
			continue
		}
		response := Dispatch(service, payload, true)
		if response != nil {
			if err := write(response); err != nil {
				return
			}
		}
		request, _ := payload.(map[string]any)
		body, _ := response.(map[string]any)
		if request["method"] == "events.subscribe" && body["result"] != nil {
			if stop != nil {
				stop()
			}
			result := body["result"].(map[string]any)
			cursor, _ := result["cursor"].(string)
			updates, cancel := service.Listen()
			streamCtx, stopStream := context.WithCancel(r.Context())
			stop = func() { stopStream(); cancel() }
			go func() {
				defer stopStream()
				defer cancel()
				streamSocket(streamCtx, service, cursor, updates, write)
			}()
		}
	}
}

func streamSocket(ctx context.Context, service *serve.Service, cursor string, updates <-chan struct{}, write func(any) error) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Catch changes between Dispatch and listener registration before waiting.
		event := service.Since(cursor)
		next, _ := event["cursor"].(string)
		if next != cursor {
			cursor = next
			if err := write(bounded(map[string]any{"jsonrpc": "2.0", "method": "events", "params": event})); err != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-updates:
			if !ok {
				return
			}
		}
	}
}

func writeSocket(ctx context.Context, conn *websocket.Conn, value any) error {
	encoded, err := marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, encoded)
}
