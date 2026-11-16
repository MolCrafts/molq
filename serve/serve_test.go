package serve

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestHello(t *testing.T) {
	service, err := Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	result, err := service.Call("molq.hello", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	body, ok := result.(hello)
	if !ok {
		t.Fatalf("result = %#v", result)
	}
	if body.RegistryID == "" || body.InstanceID == "" {
		t.Fatalf("hello = %#v", body)
	}
	encoded, _ := json.Marshal(body)
	var fields map[string]any
	_ = json.Unmarshal(encoded, &fields)
	for _, key := range []string{"version", "protocol_version", "schema_version"} {
		if _, present := fields[key]; present {
			t.Fatalf("hello contains version field %s", key)
		}
	}
	if body.Limits.MaxFrameBytes != 1048576 || body.Limits.BatchSize != 128 {
		t.Fatalf("limits = %#v", body.Limits)
	}
}
