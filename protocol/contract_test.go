package protocol

import (
	"os"
	"strings"
	"testing"
)

func TestEmbeddedContractMatchesSource(t *testing.T) {
	source, err := os.ReadFile("../src/molq/protocol/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(source) != string(Current().Raw()) {
		t.Fatal("protocol/contract.json drifted from src/molq/protocol/contract.json")
	}
	if strings.Contains(string(source), `"protocol_version"`) {
		t.Fatal("contract must not carry a protocol version")
	}
	found := false
	for _, name := range Current().Methods() {
		if name == "molq.hello" {
			found = true
		}
	}
	if !found {
		t.Fatal("molq.hello missing")
	}
	if err := Current().Validate("molq.hello", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := Current().Validate("molq.hello", map[string]any{"extra": true}); err == nil {
		t.Fatal("expected schema rejection")
	}
}
