package rpc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MolCrafts/molq/serve"
)

func TestStdioHello(t *testing.T) {
	service := open(t)
	var out bytes.Buffer
	err := Stdio(service, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"molq.hello"}`+"\n"), &out)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	result := response["result"].(map[string]any)
	if result["registry_id"] == "" || result["runtime_instance_id"] == "" {
		t.Fatalf("result = %#v", result)
	}
}

type countingReader struct {
	*strings.Reader
	reads int
}

func (r *countingReader) Read(p []byte) (int, error) { r.reads++; return r.Reader.Read(p) }
func TestFrameReaderUsesBufferedReads(t *testing.T) {
	input := &countingReader{Reader: strings.NewReader(strings.Repeat("x", maxFrame) + "\n")}
	reader := bufReader{r: bufio.NewReader(input)}
	frame, err := reader.readLine(maxFrame + 1)
	if err != nil || len(frame) != maxFrame || input.reads > 300 {
		t.Fatalf("frame=%d reads=%d err=%v", len(frame), input.reads, err)
	}
}

func TestStdioOversizedFrameIsNotSplit(t *testing.T) {
	service := open(t)
	// A request hidden after an oversized prefix belongs to that same frame.
	hello := `{"jsonrpc":"2.0","id":99,"method":"molq.hello"}`
	input := strings.Repeat(" ", maxFrame+1) + hello + "\n" + hello + "\n"
	var out bytes.Buffer
	if err := Stdio(service, strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n"))
	if len(lines) != 2 || !bytes.Contains(lines[0], []byte(`"error"`)) || !bytes.Contains(lines[1], []byte(`"result"`)) {
		t.Fatalf("oversized frame was split: %s", out.Bytes())
	}
}

func TestStdioAcceptsFrameAtLimit(t *testing.T) {
	hello := `{"jsonrpc":"2.0","id":1,"method":"molq.hello"}`
	for _, ending := range []string{"", "\n"} {
		var out bytes.Buffer
		input := hello + strings.Repeat(" ", maxFrame-len(hello)) + ending
		if err := Stdio(open(t), strings.NewReader(input), &out); err != nil || !bytes.Contains(out.Bytes(), []byte(`"result"`)) {
			t.Fatalf("frame at limit failed: %v %s", err, out.Bytes())
		}
	}
}

func TestHTTPHelloRequiresToken(t *testing.T) {
	service := open(t)
	handler, err := Handler(service, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"molq.hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["result"].(map[string]any)["registry_id"] == "" {
		t.Fatalf("body = %#v", body)
	}
}

func open(t *testing.T) *serve.Service {
	t.Helper()
	service, err := serve.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}
