package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/MolCrafts/molq/protocol"
	"github.com/mattn/go-isatty"
)

func present(view string, result any) error {
	if !forceJSON && isatty.IsTerminal(os.Stdout.Fd()) {
		if text, ok := humanView(view, result); ok {
			fmt.Fprint(os.Stdout, text)
			return nil
		}
	}
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(encoded))
	return nil
}

func humanView(view string, result any) (string, bool) {
	switch view {
	case "cluster list":
		rows, ok := result.([]any)
		if !ok {
			return "", false
		}
		return renderClusters(rows), true
	case "job list":
		body, ok := result.(map[string]any)
		if !ok {
			return "", false
		}
		return renderJobs(body), true
	default:
		return "", false
	}
}

func renderClusters(items []any) string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		cluster, _ := item.(map[string]any)
		rows = append(rows, []string{
			cell(cluster["name"]),
			cell(cluster["scheduler"]),
			transportCell(cluster["transport"]),
			cell(cluster["revision"]),
		})
	}
	return table([]string{"NAME", "SCHEDULER", "TRANSPORT", "REVISION"}, rows)
}

func renderJobs(body map[string]any) string {
	raw, _ := body["snapshots"].([]any)
	rows := make([][]string, 0, len(raw))
	for _, item := range raw {
		snap, _ := item.(map[string]any)
		ref, _ := snap["ref"].(map[string]any)
		rows = append(rows, []string{
			cell(ref["native_id"]),
			cell(snap["name"]),
			cell(snap["state"]),
			cell(snap["submitted_at"]),
		})
	}
	return table([]string{"NATIVE_ID", "NAME", "STATE", "SUBMITTED"}, rows)
}

func transportCell(value any) string {
	item, ok := value.(map[string]any)
	if !ok {
		return "local"
	}
	kind, _ := item["kind"].(string)
	alias, _ := item["alias"].(string)
	if kind == "" {
		return "local"
	}
	if alias == "" {
		return kind
	}
	return kind + " " + alias
}

func cell(value any) string {
	switch item := value.(type) {
	case nil:
		return "-"
	case string:
		if item == "" {
			return "-"
		}
		return item
	case float64:
		if item == float64(int64(item)) {
			return fmt.Sprintf("%d", int64(item))
		}
		return fmt.Sprint(item)
	default:
		text := fmt.Sprint(item)
		if text == "" || text == "<nil>" {
			return "-"
		}
		return text
	}
}

func table(headers []string, rows [][]string) string {
	width := make([]int, len(headers))
	for i, header := range headers {
		width[i] = len(header)
	}
	for _, row := range rows {
		for i, value := range row {
			if i < len(width) && len(value) > width[i] {
				width[i] = len(value)
			}
		}
	}
	var b strings.Builder
	write := func(values []string) {
		for i, value := range values {
			if i > 0 {
				b.WriteString("  ")
			}
			b.WriteString(value)
			if i < len(values)-1 && i < len(width) {
				padding := width[i] - len(value)
				if padding > 0 {
					b.WriteString(strings.Repeat(" ", padding))
				}
			}
		}
		b.WriteByte('\n')
	}
	write(headers)
	if len(rows) == 0 {
		b.WriteString("(none)\n")
		return b.String()
	}
	for _, row := range rows {
		write(row)
	}
	return b.String()
}

func formatError(err *protocol.Error) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n%s\n", err.Kind, err.Message)
	for _, key := range []string{"command", "returncode", "cause", "detail", "alias", "diagnostic"} {
		value, ok := err.Context[key]
		if !ok || value == nil {
			continue
		}
		text := fmt.Sprint(value)
		if text == "" {
			continue
		}
		if key == "diagnostic" && strings.Contains(text, "\n") {
			fmt.Fprintf(&b, "diagnostic:\n%s\n", text)
			continue
		}
		fmt.Fprintf(&b, "%s: %s\n", key, text)
	}
	return b.String()
}
