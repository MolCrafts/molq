package serve

import (
	"bytes"
	"context"
	"encoding/base64"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

const fileOpTimeout = 30 * time.Second

// Each script takes its paths as positional parameters, so no path is ever
// spliced into shell text. Precondition failures exit with the errno value
// the caller maps back to a reason before anything is changed.
const (
	statScript = `p=$1; l=0; [ -L "$p" ] && l=1
if [ ! -e "$p" ]; then printf 'missing %s\n' "$l"; exit 0; fi
if [ -d "$p" ]; then k=directory; elif [ -f "$p" ]; then k=file; else k=other; fi
m=$(date -r "$p" +%s) || exit 1
set -- $(LC_ALL=C ls -ldnL -- "$p")
printf '%s %s %s %s %s %s %s\n' "$k" "$l" "$1" "$3" "$4" "$5" "$m"`
	listScript = `[ -e "$1" ] || exit 2; [ -d "$1" ] || exit 20; cd -- "$1" || exit 1
for f in * .[!.]* ..?*; do
  l=0; [ -L "$f" ] && l=1
  if [ -d "$f" ]; then k=directory; elif [ -f "$f" ]; then k=file; elif [ -e "$f" ]; then k=other
  elif [ "$l" = 1 ]; then k=missing; else continue; fi
  printf '%s %s %s\0' "$k" "$l" "$f"
done`
	// pathlib.Path.mkdir: the requested mode is masked by the umask, and
	// `mkdir -m` alone would set it verbatim.
	mkdirScript = `p=$1
m=$(printf '%o' $(( 0$4 & ~0$(umask) )))
if [ -d "$p" ]; then [ "$3" = 1 ] && exit 0; exit 17; fi
{ [ -e "$p" ] || [ -L "$p" ]; } && exit 17
if [ "$2" = 1 ]; then mkdir -p -m "$m" -- "$p" || exit 1; exit 0; fi
[ -d "$(dirname -- "$p")" ] || exit 2
mkdir -m "$m" -- "$p" || exit 1`
	removeScript = `p=$1
[ -e "$p" ] || [ -L "$p" ] || exit 2
if [ "$3" = 1 ]; then rm -rf -- "$p" || exit 1; exit 0; fi
if [ "$2" = 1 ]; then
  [ -L "$p" ] && exit 20; [ -d "$p" ] || exit 20
  [ -z "$(ls -A -- "$p")" ] || exit 39
  rmdir -- "$p" || exit 1
else
  [ -d "$p" ] && [ ! -L "$p" ] && exit 21
  rm -f -- "$p" || exit 1
fi`
	renameScript = `[ -e "$1" ] || [ -L "$1" ] || exit 2
[ -d "$2" ] && [ ! -L "$2" ] && exit 21
mv -f -- "$1" "$2" || exit 1`
	touchScript = `if [ -e "$1" ]; then [ "$2" = 1 ] || exit 17; fi
[ -d "$(dirname -- "$1")" ] || exit 2
touch -- "$1" || exit 1`
)

var fileReasons = map[int]string{
	2: "not_found", 13: "outside_root", 17: "exists", 20: "not_directory", 21: "is_directory", 39: "not_empty",
}

// filePath binds a files.* request to its cluster transport and resolved path.
func filePath(ctx context.Context, cluster map[string]any, runner transport.Transport, params map[string]any, key string) (string, error) {
	requested := stringField(params[key])
	resolved, err := clusterPath(clusterTransport(cluster), requested)
	if err != nil {
		return "", err
	}
	if resolved == "" {
		resolved = "."
	}
	scoped := deepCopy(params).(map[string]any)
	scoped[key] = resolved
	return scopedPath(ctx, runner, scoped, key)
}

func (s *Service) files(ctx context.Context, method string, params map[string]any) (any, error) {
	cluster, runner, err := s.fileBinding(stringField(params["cluster"]))
	if err != nil {
		return nil, err
	}
	release, err := s.acquire(ctx, stringField(cluster["id"]))
	if err != nil {
		return nil, err
	}
	defer release()
	target, err := filePath(ctx, cluster, runner, params, "path")
	if err != nil {
		return nil, err
	}

	switch method {
	case "files.resolve":
		value, err := resolveFilePath(ctx, runner, target, stringField(params["root"]))
		return map[string]any{"path": value}, err
	case "files.stat":
		output, err := runFileScript(ctx, runner, false, target, statScript, target)
		if err != nil {
			return nil, err
		}
		return parseStat(target, output)
	case "files.list":
		return listFiles(ctx, runner, target, params)
	case "files.read":
		offset, _ := asInt(params["offset"])
		limit, ok := asInt(params["limit"])
		if !ok {
			limit = 262144
		}
		data, err := runner.Read(ctx, target, offset, limit)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"data": base64.StdEncoding.EncodeToString(data), "bytes_read": len(data), "next_offset": offset + len(data),
		}, nil
	case "files.write":
		return s.writeFile(ctx, runner, target, params)
	case "files.mkdir":
		mode := "777"
		if value, ok := asInt(params["mode"]); ok {
			mode = strconv.FormatInt(int64(value), 8)
		}
		_, err = runFileScript(ctx, runner, true, target, mkdirScript, target, flag(params["parents"]), flag(params["exist_ok"]), mode)
	case "files.remove":
		_, err = runFileScript(ctx, runner, true, target, removeScript, target, flag(params["directory"]), flag(params["recursive"]))
	case "files.chmod":
		mode, _ := asInt(params["mode"])
		_, err = runFileScript(ctx, runner, true, target, `chmod -- "$2" "$1"`, target, strconv.FormatInt(int64(mode), 8))
	case "files.symlink":
		linkTarget := stringField(params["target"])
		scoped := deepCopy(params).(map[string]any)
		if !strings.HasPrefix(linkTarget, "/") {
			scoped["target"] = path.Dir(target) + "/" + linkTarget
		}
		if _, err := filePath(ctx, cluster, runner, scoped, "target"); err != nil {
			return nil, err
		}
		_, err = runFileScript(ctx, runner, true, target, `ln -s -- "$2" "$1"`, target, linkTarget)

	case "files.touch":
		_, err = runFileScript(ctx, runner, true, target, touchScript, target, flag(params["exist_ok"]))
	case "files.rename":
		other, bindErr := filePath(ctx, cluster, runner, params, "target")
		if bindErr != nil {
			return nil, bindErr
		}
		_, err = runFileScript(ctx, runner, true, target, renameScript, target, other)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"outcome": "applied"}, nil
}

func (s *Service) writeFile(ctx context.Context, runner transport.Transport, target string, params map[string]any) (any, error) {
	data, err := base64.StdEncoding.DecodeString(stringField(params["data"]))
	if err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "File data must be base64", nil)
	}
	var offset *int
	if value, ok := asInt(params["offset"]); ok {
		offset = &value
	}
	if err := runner.Write(ctx, target, data, offset); err != nil {
		return nil, err
	}
	return map[string]any{"outcome": "applied", "bytes_written": len(data)}, nil
}

// runFileScript runs one POSIX sh script on the target. A mutating script
// that was dispatched without a definite answer has an unknown outcome.
func runFileScript(ctx context.Context, runner transport.Transport, mutation bool, target, script string, args ...string) ([]byte, error) {
	argv := append([]string{"sh", "-c", script, "sh"}, args...)
	result, err := runner.Run(ctx, argv, fileOpTimeout, nil, false)
	if err != nil {
		if failure, ok := err.(*protocol.Error); ok && mutation && failure.Context["dispatched"] == true {
			return nil, protocol.FailOutcome("OUTCOME_UNKNOWN", "Target file operation outcome is uncertain", "unknown", failure.Context)
		}
		return nil, err
	}
	if result.ReturnCode == 0 {
		return result.Stdout, nil
	}
	context := map[string]any{"path": target, "returncode": result.ReturnCode}
	if reason, ok := fileReasons[result.ReturnCode]; ok {
		context["reason"] = reason
		return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Target file operation precondition failed", "not_applied", context)
	}
	context["stderr"] = strings.ToValidUTF8(string(result.Stderr), "�")
	outcome := "not_applied"
	if mutation {
		outcome = "unknown"
	}
	return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Target file operation failed", outcome, context)
}

func parseStat(target string, output []byte) (any, error) {
	fields := strings.Fields(string(output))
	if len(fields) == 2 && fields[0] == "missing" {
		return map[string]any{"exists": false, "is_symlink": fields[1] == "1"}, nil
	}
	if len(fields) != 7 {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Unrecognised target stat output", map[string]any{"path": target})
	}
	number := func(text string) int64 {
		value, _ := strconv.ParseInt(text, 10, 64)
		return value
	}
	return map[string]any{
		"exists": true, "kind": fields[0], "is_symlink": fields[1] == "1",
		"mode": permissionBits(fields[2]), "uid": number(fields[3]), "gid": number(fields[4]),
		"size": number(fields[5]), "mtime": number(fields[6]),
	}, nil
}

func parseEntries(output []byte) []map[string]any {
	entries := []map[string]any{}
	for _, record := range bytes.Split(output, []byte{0}) {
		parts := strings.SplitN(string(record), " ", 3)
		if len(parts) != 3 {
			continue
		}
		entries = append(entries, map[string]any{"name": parts[2], "kind": parts[0], "is_symlink": parts[1] == "1"})
	}
	return entries
}

// permissionBits reads the nine rwx columns of an `ls -l` mode string,
// including setuid, setgid and sticky markers.
func permissionBits(text string) int {
	if len(text) < 10 {
		return 0
	}
	mode := 0
	for i, char := range text[1:10] {
		shift := 8 - i
		switch char {
		case 'r', 'w', 'x':
			mode |= 1 << shift
		case 's', 't':
			mode |= 1 << shift
			fallthrough
		case 'S', 'T':
			mode |= [3]int{0o4000, 0o2000, 0o1000}[i/3]
		}
	}
	return mode
}

func flag(value any) string {
	if value == true {
		return "1"
	}
	return "0"
}
