package serve

import (
	"fmt"
	"os"
	"testing"

	"github.com/MolCrafts/molq/transport"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--file-io" {
		if err := transport.FileIO(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	// Job directories live under the target home; keep them out of the real one.
	home, err := os.MkdirTemp("", "molq-serve-home-")
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	code := m.Run()
	os.RemoveAll(home)
	os.Exit(code)
}
