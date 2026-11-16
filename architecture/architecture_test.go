// Package architecture enforces the public RPC boundary between clients and serve.
package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/MolCrafts/molq/"

func TestClientsDoNotImportServiceImplementation(t *testing.T) {
	for _, directory := range []string{"client", "watch", "cmd/molq"} {
		checkImports(t, directory, func(path, imported string) bool {
			if path == "cmd/molq/serve.go" {
				return false // The serve entry point alone assembles the service.
			}
			for _, service := range []string{"serve", "registry", "scheduler", "transport", "rpc"} {
				if imported == module+service || strings.HasPrefix(imported, module+service+"/") {
					return true
				}
			}
			return false
		})
	}
}

func TestServiceDoesNotImportClientFacades(t *testing.T) {
	for _, directory := range []string{"serve", "registry", "scheduler", "transport", "rpc"} {
		checkImports(t, directory, func(_, imported string) bool {
			return imported == module+"client" || imported == module+"watch" ||
				strings.HasPrefix(imported, module+"client/") || strings.HasPrefix(imported, module+"watch/")
		})
	}
}

func checkImports(t *testing.T, directory string, forbidden func(string, string) bool) {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Cannot locate repository")
	}
	root := filepath.Dir(filepath.Dir(filename))
	err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, item := range file.Imports {
			imported, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				return err
			}
			if forbidden(filepath.ToSlash(relative), imported) {
				t.Errorf("%s imports %s across the public RPC boundary", relative, imported)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
