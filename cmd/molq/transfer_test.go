package main

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestTransferParamsFollowRsyncOperands(t *testing.T) {
	params, err := transferParams([]string{"-avz", "--delete", "--exclude", "build/", "-f", "- *.o", "hpc:runs/a/", "hpc:/b", "out/"})
	if err != nil {
		t.Fatal(err)
	}
	wantOptions := []any{"-avz", "--delete", "--exclude=build/", "--filter=- *.o"}
	if !reflect.DeepEqual(params["options"], wantOptions) {
		t.Fatalf("options = %#v", params["options"])
	}
	sources := params["sources"].([]any)
	if !reflect.DeepEqual(sources, []any{
		map[string]any{"cluster": "hpc", "path": "runs/a/"}, map[string]any{"cluster": "hpc", "path": "/b"},
	}) {
		t.Fatalf("sources = %#v", sources)
	}
	destination := params["destination"].(map[string]any)
	absolute, _ := filepath.Abs("out")
	if destination["path"] != filepath.ToSlash(absolute)+"/" || destination["cluster"] != nil {
		t.Fatalf("destination = %#v", destination)
	}
}

func TestTransferParamsRejectTransportOwnedOptions(t *testing.T) {
	for _, args := range [][]string{
		{"-e", "ssh -p 2", "a", "hpc:b"},
		{"-avze", "ssh", "a", "hpc:b"},
		{"--rsync-path=sudo rsync", "a", "hpc:b"},
		{"a", "host::module"},
		{"only-one-operand"},
	} {
		_, err := transferParams(args)
		if failure, ok := err.(*protocol.Error); !ok || failure.Kind != "INVALID_INPUT" {
			t.Fatalf("%v error = %#v", args, err)
		}
	}
}

func TestTransferHelpLeavesRsyncHumanReadable(t *testing.T) {
	if wantsHelp([]string{"transfer", "-avh", "a", "b"}) {
		t.Fatal("-h inside an rsync bundle must not open help")
	}
	if !wantsHelp([]string{"transfer", "-h"}) || !wantsHelp([]string{"transfer"}) {
		t.Fatal("bare transfer -h opens help")
	}
}
