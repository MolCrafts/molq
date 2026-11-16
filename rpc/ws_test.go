package rpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MolCrafts/molq/serve"
	"github.com/coder/websocket"
)

func TestWebSocketRejectsMissingToken(t *testing.T) {
	service, err := serve.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	handler, err := Handler(service, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+server.URL[4:]+"/rpc", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer 0123456789abcdef"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeCtx, cancelWrite := context.WithTimeout(ctx, time.Second)
	defer cancelWrite()
	if err := conn.Write(writeCtx, websocket.MessageText, []byte(`{"token":"wrong"}`)); err != nil {
		t.Fatal(err)
	}
	_, message, err := conn.Read(ctx)
	if err != nil || !bytes.Contains(message, []byte("AUTH_REQUIRED")) {
		t.Fatalf("message = %s err = %v", message, err)
	}
}

func TestWebSocketAcceptsContractFrameLimit(t *testing.T) {
	service := open(t)
	handler, err := Handler(service, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+server.URL[4:]+"/rpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"token":"0123456789abcdef"}`)); err != nil {
		t.Fatal(err)
	}
	hello := `{"jsonrpc":"2.0","id":7,"method":"molq.hello"}`
	frame := hello + strings.Repeat(" ", maxFrame-len(hello))
	if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	_, message, err := conn.Read(ctx)
	if err != nil || !bytes.Contains(message, []byte(`"result"`)) {
		t.Fatalf("message=%s err=%v", message, err)
	}
}
