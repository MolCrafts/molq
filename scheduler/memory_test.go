package scheduler

import (
	"strings"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestMemoryDirectiveRoundingDoesNotOverflow(t *testing.T) {
	for _, test := range []struct{ bytes, mib string }{
		{"1", "1"},
		{"1048575", "1"},
		{"1048576", "1"},
		{"1048577", "2"},
		{"2147483647", "2048"},
		{"2147483648", "2048"},
		{"9223372036853727232", "8796093022207"},
		{"9223372036854775807", "8796093022208"},
		{"9223372036854775808", "8796093022208"},
		{"18446744073709551615", "17592186044416"},
		{"18446744073709551616", "17592186044416"},
		{"18446744073709551617", "17592186044417"},
		{"99999999999999999999", "95367431640625"},
	} {
		t.Run(test.bytes, func(t *testing.T) {
			for _, native := range []struct {
				scheduler Scheduler
				directive string
			}{
				{NewSlurm(script(), map[string]any{}), "--mem=" + test.mib + "M\n"},
				{NewPBS(script(), map[string]any{"pbs_dialect": "openpbs"}), ":mem=" + test.mib + "mb\n"},
			} {
				output, err := native.scheduler.Render(specWith(map[string]any{"memory_bytes": test.bytes}, nil, nil), "/target")
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(output, native.directive) {
					t.Fatalf("missing %s in %s", native.directive, output)
				}
			}
		})
	}
}

func TestMemoryInvalidValuesFailBeforeRendering(t *testing.T) {
	for _, value := range []any{"", "0", "01", "-1", "+1", "1e6", "123x", "100000000000000000000", "١", float64(1048576), nil} {
		for _, native := range []Scheduler{
			NewSlurm(script(), map[string]any{}),
			NewPBS(script(), map[string]any{"pbs_dialect": "openpbs"}),
		} {
			spec := specWith(map[string]any{"memory_bytes": value}, nil, nil)
			for _, err := range []error{native.Validate(spec), renderMemoryError(native, spec)} {
				failure, ok := err.(*protocol.Error)
				if !ok || failure.Kind != "RESOURCE_REQUEST_INVALID" || failure.Outcome != "not_applied" {
					t.Fatalf("memory %v: %#v", value, err)
				}
			}
		}
	}
}

func renderMemoryError(native Scheduler, spec map[string]any) error {
	_, err := native.Render(spec, "/target")
	return err
}

func TestAbsentMemoryDoesNotRequestNativeUnlimitedMemory(t *testing.T) {
	for _, native := range []Scheduler{
		NewSlurm(script(), map[string]any{}),
		NewPBS(script(), map[string]any{"pbs_dialect": "openpbs"}),
	} {
		output, err := native.Render(specWith(nil, nil, nil), "/target")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output, "--mem=") || strings.Contains(output, ":mem=") {
			t.Fatalf("unexpected memory directive: %s", output)
		}
	}
}
