package serve

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

const pagedListScript = resolvePathScript + `export LC_ALL=C
target=$1; take=$2; after=$3; withstat=$4; root=$5
[ -e "$target" ] || exit 2; [ -d "$target" ] || exit 20
cd -- "$target" || exit 1
# Each glob is sorted by the target shell. Return a bounded prefix from each
# disjoint group; the service merges these three windows in byte order.
for group in parents hidden normal; do
 case "$group" in parents) set -- ..?*;; hidden) set -- .[!.]*;; normal) set -- *;; esac
 count=0
 for f do
  [ -e "$f" ] || [ -L "$f" ] || continue
  [ -z "$after" ] || [ "$f" \> "$after" ] || continue
  count=$((count+1)); [ "$count" -le "$take" ] || break
  link=0; [ -L "$f" ] && link=1
  kind=other; size=0; mtime=0
  if [ -d "$f" ]; then kind=directory; elif [ -f "$f" ]; then kind=file
  elif [ "$link" = 1 ]; then kind=missing; fi
  if [ -n "$root" ] && ! resolve "$PWD/$f" "$root" >/dev/null; then kind=missing;
  elif [ "$withstat" = 1 ] && [ -e "$f" ]; then
    # uutils date parses an operand like -dash as an option; ./ keeps it a file.
    mtime=$(date -r "./$f" +%s) || exit 1
    set -- $(LC_ALL=C ls -ldnL -- "$f"); size=$5
  fi
  printf '%s %s %s %s %s\000' "$kind" "$link" "$size" "$mtime" "$f"
 done
done`

func listFiles(ctx context.Context, runner transport.Transport, target string, params map[string]any) (any, error) {
	limit, ok := asInt(params["limit"])
	if !ok {
		limit = 256
	}
	output, err := runFileScript(ctx, runner, false, target, pagedListScript,
		target, strconv.Itoa(limit+1), stringField(params["after"]), flag(params["with_stat"]), stringField(params["root"]))
	if err != nil {
		return nil, err
	}
	entries := []map[string]any{}
	for _, record := range strings.Split(string(output), "\x00") {
		if record == "" {
			continue
		}
		fields := strings.SplitN(record, " ", 5)
		if len(fields) != 5 {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Invalid directory record", nil)
		}
		if !utf8.ValidString(fields[4]) {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Directory contains a non-UTF-8 name", map[string]any{"reason": "encoding"})
		}
		entry := map[string]any{"name": fields[4], "kind": fields[0], "is_symlink": fields[1] == "1"}
		if params["with_stat"] == true {
			size, e1 := strconv.ParseInt(fields[2], 10, 64)
			mtime, e2 := strconv.ParseInt(fields[3], 10, 64)
			if e1 != nil || e2 != nil {
				return nil, protocol.Fail("FILE_UNAVAILABLE", "Invalid directory metadata", nil)
			}
			entry["size"], entry["mtime"] = size, mtime
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i]["name"].(string) < entries[j]["name"].(string) })
	var next any
	if len(entries) > limit {
		entries = entries[:limit]
		next = entries[len(entries)-1]["name"]
	}
	return map[string]any{"entries": entries, "next_after": next}, nil
}

// One target command handles the complete batch. A file is read only when its
// complete payload fits both budgets; partial contents are never published.
const readManyScript = resolvePathScript + `root=$1; cap=$2; shift 2; remaining=524288
cd "$HOME" || exit 1
for requested do
 name=$(printf '%s' "$requested" | base64 | tr -d '\n')
 if p=$(resolve "$requested" "$root"); then :; else printf '%s outside_root -\n' "$name"; continue; fi
 if [ ! -e "$p" ]; then printf '%s not_found -\n' "$name"; continue; fi
 if [ ! -f "$p" ]; then printf '%s is_directory -\n' "$name"; continue; fi
 size=$(wc -c < "$p") || { printf '%s unavailable -\n' "$name"; continue; }
 if [ "$size" -gt "$cap" ] || [ "$size" -gt "$remaining" ]; then printf '%s limit -\n' "$name"; continue; fi
 # The temporary file pins this read's content so a growing input cannot
 # defeat the limits between stat and encoding.
 tmp=$(mktemp) || exit 1
 trap 'rm -f -- "$tmp"' EXIT HUP INT TERM
 if ! dd if="$p" of="$tmp" bs=1 count=$((cap+1)) 2>/dev/null; then rm -f -- "$tmp"; printf '%s unavailable -\n' "$name"; continue; fi
 size=$(wc -c < "$tmp")
 if [ "$size" -gt "$cap" ] || [ "$size" -gt "$remaining" ]; then rm -f -- "$tmp"; printf '%s limit -\n' "$name"; continue; fi
 printf '%s ok ' "$name"; base64 < "$tmp" | tr -d '\n'; printf '\n'
 rm -f -- "$tmp"; remaining=$((remaining-size))
done`

func (s *Service) readMany(ctx context.Context, params map[string]any) (any, error) {
	definition, runner, err := s.fileBinding(stringField(params["cluster"]))
	if err != nil {
		return nil, err
	}
	release, err := s.acquire(ctx, stringField(definition["id"]))
	if err != nil {
		return nil, err
	}
	defer release()
	cap, ok := asInt(params["max_bytes"])
	if !ok {
		cap = 262144
	}
	args := []string{stringField(params["root"]), strconv.Itoa(cap)}
	for _, item := range anySlice(params["paths"]) {
		args = append(args, item.(string))
	}
	output, err := runFileScript(ctx, runner, false, "", readManyScript, args...)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	budget := 900000
	reserved := map[string]int{}
	for _, value := range anySlice(params["paths"]) {
		requested := value.(string)
		encoded, _ := json.Marshal(map[string]any{"path": requested})
		size := len(encoded) + 256
		reserved[requested] = size
		budget -= size
	}
	if budget < 0 {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Batch paths exceed response capacity", map[string]any{"reason": "limit"})
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(output), "\n"), "\n") {
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Invalid batch response", nil)
		}
		name, err := base64.StdEncoding.DecodeString(fields[0])
		if err != nil {
			return nil, err
		}
		item := map[string]any{"path": string(name)}
		if fields[1] == "ok" {
			item["data"] = fields[2]
		} else {
			item["error"] = protocol.Fail("FILE_UNAVAILABLE", "Batch item could not be read", map[string]any{"reason": fields[1]}).Data()
		}
		encoded, _ := json.Marshal(item)
		used := max(0, len(encoded)-reserved[string(name)])
		if used > budget {
			delete(item, "data")
			item["error"] = protocol.Fail("FILE_UNAVAILABLE", "Batch response limit reached", map[string]any{"reason": "limit"}).Data()
			used = 0
		}
		budget -= used
		items = append(items, item)
	}
	return map[string]any{"items": items}, nil
}
