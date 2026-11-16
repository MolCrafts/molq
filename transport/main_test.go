package transport

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--file-io" {
		if err := FileIO(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
