package protocol

import (
	"reflect"
	"testing"
)

func TestSplitRsyncArgsAttachesValues(t *testing.T) {
	options, operands, err := SplitRsyncArgs([]string{"-aB4096", "--exclude", "x", "--include=y", "-Tdir", "src", "--", "-odd", "dst"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(options, []string{"-a", "--block-size=4096", "--exclude=x", "--include=y", "--temp-dir=dir"}) {
		t.Fatalf("options = %#v", options)
	}
	if !reflect.DeepEqual(operands, []string{"src", "-odd", "dst"}) {
		t.Fatalf("operands = %#v", operands)
	}
	if _, _, err := SplitRsyncArgs([]string{"--exclude"}); err == nil {
		t.Fatal("dangling value option accepted")
	}
}

func TestCheckRsyncOption(t *testing.T) {
	for _, ok := range []string{"-a", "-avzh", "--delete", "--exclude=build/", "-f- *.o", "--info=progress2"} {
		if err := CheckRsyncOption(ok); err != nil {
			t.Fatalf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"--", "a", "-", "--exclude", "-f", "-e", "-aessh", "--rsh=ssh", "--rsync-path=x", "-Mx", "--daemon", "--server", "-a\n"} {
		if err := CheckRsyncOption(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
