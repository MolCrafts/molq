package serve

import (
	"context"
	"strings"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

// Resolve on the destination, never against the controller's home or symlinks.
// Existing symlinks are followed; missing leaves retain their physical parent.
const resolvePathScript = `canonical() {
 p=$1; case "$p" in '~') p=$HOME;; '~/'*) p=$HOME/${p#\~/};; esac
 case "$p" in /*) ;; *) p=$PWD/$p;; esac
 n=0
 while [ -L "$p" ]; do
   n=$((n+1)); [ "$n" -le 40 ] || return 40
   link=$(readlink "$p") || return 1
   case "$link" in /*) p=$link;; *) p=$(dirname -- "$p")/$link;; esac
 done
 if [ -d "$p" ]; then (cd -P -- "$p" && pwd -P); return; fi
 leaf=$(basename -- "$p"); parent=$(dirname -- "$p"); tail=
 while [ ! -d "$parent" ]; do
   next=$(dirname -- "$parent"); [ "$next" != "$parent" ] || return 2
   tail=$(basename -- "$parent")/$tail; parent=$next
 done
 parent=$(cd -P -- "$parent" && pwd -P) || return 1
 printf '%s/%s%s\n' "${parent%/}" "$tail" "$leaf"
}
resolve() {
 if [ -n "$2" ]; then case "/$1/" in */../*) return 13;; esac; fi
 value=$(canonical "$1") || return $?
 if [ -n "$2" ]; then
   base=$(canonical "$2") || return $?
   case "$value" in "$base"|"${base%/}"/*) ;; *) return 13;; esac
 fi
 printf '%s\n' "$value"
}
`

func resolveFilePath(ctx context.Context, runner transport.Transport, requested, root string) (string, error) {
	output, err := runFileScript(ctx, runner, false, requested,
		resolvePathScript+`resolve "$1" "$2"`, requested, root)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(output), "\n"), nil
}

func (s *Service) fileBinding(name string) (map[string]any, transport.Transport, error) {
	definition, err := s.Registry.Get(name)
	if err != nil {
		return nil, nil, err
	}
	return definition, transport.Create(clusterTransport(definition)), nil
}

func scopedPath(ctx context.Context, runner transport.Transport, value map[string]any, key string) (string, error) {
	requested := stringField(value[key])
	if strings.ContainsAny(requested, "\n\r\x00") {
		return "", protocol.Fail("INVALID_INPUT", "File paths must be single-line", nil)
	}
	root := stringField(value["root"])
	if root != "" {
		if _, err := resolveFilePath(ctx, runner, requested, root); err != nil {
			return "", err
		}
		return requested, nil
	}
	return requested, nil
}

// A scoped directory transfer must validate links below the operand as well:
// rsync may dereference them, including directory links to another in-root tree.
// The native deadline bounds repeated/cyclic links; no file contents are read.
// The script passes its own text to find, and GNU find allows one "{}" per
// -exec ... + clause in any argument, so the text spells the operand {""}.
const transferScopeValidator = resolvePathScript + `root=$1; depth=$2; validator=$3; shift 3
[ "$depth" -le 40 ] || exit 13
for candidate do
 resolved=$(resolve "$candidate" "$root") || exit 13
 if [ -d "$resolved" ]; then
   find "$resolved" -type l -exec sh -c "$validator" sh "$root" "$((depth+1))" "$validator" {""} + || exit 13
 fi
done`

func validateScopedTransfer(ctx context.Context, runner transport.Transport, requested, root string) error {
	_, err := runFileScript(ctx, runner, false, requested, transferScopeValidator, root, "0", transferScopeValidator, requested)
	return err
}
