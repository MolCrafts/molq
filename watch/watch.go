// Package watch periodically asks serve for changes and delivers notifications.
package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// Options is one watch pass. Zero interval means five seconds.
type Options struct {
	Interval      time.Duration
	Clusters      []string
	ProjectRoot   string
	NerveEndpoint string
	Once          bool
	Out           io.Writer
}

// Run calls jobs.observe on the serve process. Notification failure does not
// change submitted work.
type Caller interface {
	CallContext(context.Context, string, map[string]any) (any, error)
}

// ReportedError marks a failure already emitted to the output stream.
// CLI callers use it to choose an exit status without printing it twice.
type ReportedError struct{ Err error }

func (e *ReportedError) Error() string { return e.Err.Error() }
func (e *ReportedError) Unwrap() error { return e.Err }

func Run(ctx context.Context, service Caller, options Options) error {
	if options.Interval <= 0 {
		options.Interval = 5 * time.Second
	}
	if options.Out == nil {
		options.Out = io.Discard
	}
	backoff := options.Interval
	sendNotification, stopNotifications := notifications(ctx, options.NerveEndpoint)
	defer stopNotifications()
	cursors := map[string]string{}
	reported := map[string]string{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names := options.Clusters
		if len(names) == 0 {
			listed, err := service.CallContext(ctx, "clusters.list", map[string]any{})
			if err != nil {
				if writeErr := reportError(options.Out, reported, "", err); writeErr != nil {
					return writeErr
				}
				if options.Once {
					return &ReportedError{Err: err}
				}
				if !sleep(ctx, backoff) {
					return ctx.Err()
				}
				backoff = nextBackoff(backoff, options.Interval)
				continue
			}
			for _, item := range mapList(listed) {
				names = append(names, stringField(item["name"]))
			}
		}
		failed := false
		var firstError error
		for _, name := range names {
			params := map[string]any{"cluster": name}
			if options.ProjectRoot != "" {
				params["project_root"] = options.ProjectRoot
			}
			if cursor := cursors[name]; cursor != "" {
				params["cursor"] = cursor
			}
			observed, err := service.CallContext(ctx, "jobs.observe", params)
			if err != nil {
				failed = true
				if firstError == nil {
					firstError = err
				}
				if writeErr := reportError(options.Out, reported, name, err); writeErr != nil {
					return writeErr
				}
				continue
			}
			delete(reported, name)
			body := observed.(map[string]any)
			cursors[name] = stringField(body["cursor"])
			for _, change := range mapList(body["changes"]) {
				if err := writeEvent(options.Out, change); err != nil {
					return err
				}
				sendNotification(change)
			}

		}
		if options.Once {
			if firstError != nil {
				return &ReportedError{Err: firstError}
			}
			return nil
		}
		wait := options.Interval
		if failed {
			wait = backoff
			backoff = nextBackoff(backoff, options.Interval)
		} else {
			backoff = options.Interval
		}
		if !sleep(ctx, wait) {
			return ctx.Err()
		}
	}
}

// Notifications run independently of sampling. A slow recipient may lose
// notifications when the finite queue fills; observed events remain on Out.
func notifications(parent context.Context, endpoint string) (func(map[string]any), func()) {
	if endpoint == "" {
		return func(map[string]any) {}, func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	queue := make(chan map[string]any, 128)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-queue:
				if !ok {
					return
				}
				notify(ctx, endpoint, event)
			}
		}
	}()
	return func(event map[string]any) {
			select {
			case queue <- event:
			default:
			}
		}, func() {
			close(queue)
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-done:
			case <-parent.Done():
			case <-timer.C:
			}
			cancel()
			<-done
		}
}

func mapList(value any) []map[string]any {
	switch items := value.(type) {
	case []map[string]any:
		return items
	case []any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			if event, ok := item.(map[string]any); ok {
				out = append(out, event)
			}
		}
		return out
	default:
		return nil
	}
}

func writeEvent(out io.Writer, event map[string]any) error {
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "events", "params": event})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(encoded))
	return err
}

func reportError(out io.Writer, reported map[string]string, cluster string, err error) error {
	signature := errorSignature(err)
	if reported[cluster] == signature {
		return nil
	}
	if writeErr := writeError(out, err); writeErr != nil {
		return writeErr
	}
	reported[cluster] = signature
	return nil
}

func errorSignature(err error) string {
	item, ok := err.(*protocol.Error)
	if !ok {
		return err.Error()
	}
	encoded, _ := json.Marshal(item.Data())
	return string(encoded)
}

func writeError(out io.Writer, err error) error {
	item, ok := err.(*protocol.Error)
	if !ok {
		item = protocol.Fail("INTERNAL_ERROR", err.Error(), nil)
	}
	encoded, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": nil,
		"error": map[string]any{"code": -32000, "message": item.Error(), "data": item.Data()},
	})
	_, writeErr := fmt.Fprintln(out, string(encoded))
	return writeErr
}

func notify(parent context.Context, endpoint string, event map[string]any) {
	if endpoint == "" {
		return
	}
	body, err := json.Marshal(event)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
	_ = response.Body.Close()
}

func sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, interval time.Duration) time.Duration {
	next := current * 2
	limit := interval * 8
	if limit < time.Minute {
		limit = time.Minute
	}
	if next > limit {
		return limit
	}
	return next
}

func stringField(value any) string {
	text, _ := value.(string)
	return text
}
