package transport

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func localSSH(t *testing.T) SSH {
	t.Helper()
	requirePOSIX(t)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte("#!/bin/sh\nfor argument; do remote=$argument; done\nexec sh -c \"$remote\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return SSH{Alias: "fixture"}
}

func TestSSHReadExactChunks(t *testing.T) {
	remote := localSSH(t)
	data := make([]byte, 600000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	file := filepath.Join(t.TempDir(), "data with ' quote")
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ offset, limit int }{
		{0, 262144}, {1, 262144}, {65535, 262144}, {65536, 262144},
		{131071, 1}, {599990, 262144}, {700000, 262144}, {0, 0},
	} {
		got, err := remote.Read(context.Background(), file, test.offset, test.limit)
		if err != nil {
			t.Fatal(err)
		}
		start := min(test.offset, len(data))
		end := min(test.offset+test.limit, len(data))
		if !bytes.Equal(got, data[start:end]) {
			t.Fatalf("chunk offset=%d limit=%d: got %d bytes, want %d", test.offset, test.limit, len(got), end-start)
		}
	}
}

func TestSSHWriteExactOffsetsPreservesOtherBytes(t *testing.T) {
	remote := localSSH(t)
	for _, offset := range []int{0, 17, 65536, 131072} {
		original := bytes.Repeat([]byte("o"), 200000)
		payload := bytes.Repeat([]byte("n"), 65537)
		file := filepath.Join(t.TempDir(), "data")
		if err := os.WriteFile(file, original, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := remote.Write(context.Background(), file, payload, &offset); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		copy(original[offset:], payload)
		if !bytes.Equal(got, original) {
			t.Fatalf("offset=%d changed bytes outside requested chunk", offset)
		}
	}
}
