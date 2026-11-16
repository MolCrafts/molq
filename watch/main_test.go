package watch

import (
	"os"
	"testing"
)

// Job directories live under the target home; keep them out of the real one.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "molq-watch-home-")
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	code := m.Run()
	os.RemoveAll(home)
	os.Exit(code)
}
