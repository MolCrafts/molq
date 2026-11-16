package scheduler

import (
	"math/big"

	"github.com/MolCrafts/molq/protocol"
)

// memoryMiB rounds decimal byte requests upward without host integer overflow.
// Every schema-valid 20-digit byte count yields a MiB count below 2^63. Keep
// that count as decimal text so rendering does not depend on host int width.
func memoryMiB(resources map[string]any) (string, error) {
	value, present := resources["memory_bytes"]
	if !present {
		return "", nil
	}
	memory, ok := value.(string)
	if !ok || len(memory) == 0 || len(memory) > 20 || memory[0] < '1' || memory[0] > '9' {
		return "", invalidMemory()
	}
	for _, digit := range memory {
		if digit < '0' || digit > '9' {
			return "", invalidMemory()
		}
	}
	bytes, _ := new(big.Int).SetString(memory, 10)
	const bytesPerMiB = 1 << 20
	bytes.Add(bytes, big.NewInt(bytesPerMiB-1))
	bytes.Quo(bytes, big.NewInt(bytesPerMiB))
	return bytes.String(), nil
}

func invalidMemory() error {
	return protocol.Fail("RESOURCE_REQUEST_INVALID", "memory_bytes must be a positive decimal string of at most 20 digits", nil)
}
