package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

type submissionReceipt struct {
	Fingerprint string
	ClusterID   string
	Revision    int
	Lookup      scheduler.SubmissionLookup
	Result      map[string]any
	Failure     map[string]any
}

func (s *Service) submissionPath(key string) string {
	return filepath.Join(filepath.Dir(s.Registry.Path), "submissions", s.Registry.ID, fingerprint("request_key", map[string]any{"key": key})+".json")
}

func (s *Service) readSubmission(key string) (submissionReceipt, error) {
	var receipt submissionReceipt
	name := s.submissionPath(key)
	body, err := os.ReadFile(name)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(body, &receipt); err != nil {
		return receipt, err
	}
	if receipt.Fingerprint == "" || receipt.ClusterID == "" || receipt.Lookup.Directory == "" || receipt.Lookup.Since.IsZero() {
		return receipt, fmt.Errorf("invalid submission receipt")
	}
	body, err = os.ReadFile(name + ".result")
	if err == nil {
		var result submissionReceipt
		if err := json.Unmarshal(body, &result); err != nil || result.Fingerprint != receipt.Fingerprint {
			return receipt, fmt.Errorf("invalid submission result")
		}
		return result, nil
	}
	if !os.IsNotExist(err) {
		return receipt, err
	}
	body, err = os.ReadFile(name + ".native-id")
	if err == nil {
		err = json.Unmarshal(body, &receipt.Lookup.NativeID)
	} else if os.IsNotExist(err) {
		err = nil
	}
	return receipt, err
}

func submissionStoreError(key string, err error) error {
	return protocol.FailOutcome("OUTCOME_UNKNOWN", "Cannot verify submission receipt", "unknown", map[string]any{"request_key": key, "detail": err.Error()})
}

func (s *Service) submitOnce(ctx context.Context, key string, params, spec, cluster map[string]any, item scheduler.Scheduler, directory string, revision int) (any, error) {
	scheduling := mapValue(spec["scheduling"])
	receipt := submissionReceipt{
		Fingerprint: fingerprint("jobs.submit", params),
		ClusterID:   stringField(cluster["id"]), Revision: revision,
		Lookup: scheduler.SubmissionLookup{Directory: directory, Since: time.Now().UTC(), Name: stringField(scheduling["name"])},
	}
	for _, raw := range anySlice(mapValue(scheduling["array"])["indices"]) {
		index, _ := asInt(raw)
		receipt.Lookup.Indices = append(receipt.Lookup.Indices, index)
	}
	sort.Ints(receipt.Lookup.Indices)
	name := s.submissionPath(key)
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		return nil, submissionStoreError(key, err)
	}
	if err := syncDirectory(filepath.Dir(filepath.Dir(name))); err != nil {
		return nil, submissionStoreError(key, err)
	}
	if err := syncDirectory(filepath.Dir(filepath.Dir(filepath.Dir(name)))); err != nil {
		return nil, submissionStoreError(key, err)
	}
	if err := writeReceipt(name, receipt, true); err != nil {
		// Another serve instance may have won the exclusive publication.
		if os.IsExist(err) {
			existing, readErr := s.readSubmission(key)
			if readErr == nil {
				return receiptReplay(params, existing)
			}
		}
		return nil, submissionStoreError(key, err)
	}
	job, err := item.Submit(ctx, spec, directory)
	if err != nil {
		if failure, ok := err.(*protocol.Error); ok {
			receipt.Lookup.NativeID = stringField(failure.Context["native_id"])
			if failure.Outcome == "not_applied" {
				receipt.Failure = failure.Data()
			}
		}
		if receipt.Failure != nil {
			if writeErr := writeReceipt(name+".result", receipt, true); writeErr != nil && !os.IsExist(writeErr) {
				return nil, submissionStoreError(key, writeErr)
			}
		} else if receipt.Lookup.NativeID != "" {
			if writeErr := writeReceipt(name+".native-id", receipt.Lookup.NativeID, true); writeErr != nil && !os.IsExist(writeErr) {
				return nil, submissionStoreError(key, writeErr)
			}
		}
		return nil, err
	}
	result, err := s.submissionResult(cluster, job, revision)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	receipt.Result = result
	return s.publishSubmissionResult(key, params, receipt)
}

func receiptReplay(params map[string]any, receipt submissionReceipt) (any, error) {
	if receipt.Fingerprint != fingerprint("jobs.submit", params) {
		return nil, protocol.Fail("CONFLICT", "request_key was already used for a different submission", nil)
	}
	if receipt.Result != nil {
		return receipt.Result, nil
	}
	if receipt.Failure != nil {
		return nil, fromData(receipt.Failure)
	}
	return nil, protocol.FailOutcome("OUTCOME_UNKNOWN", "Submission is awaiting native evidence", "unknown", map[string]any{"native_id": receipt.Lookup.NativeID})
}

func (s *Service) recoverSubmission(ctx context.Context, params map[string]any, receipt submissionReceipt) (any, error) {
	if receipt.Fingerprint != fingerprint("jobs.submit", params) || receipt.Result != nil || receipt.Failure != nil {
		return receiptReplay(params, receipt)
	}
	cluster, item, err := s.cluster(receipt.ClusterID)
	if err != nil {
		return receiptReplay(params, receipt)
	}
	release, err := s.acquire(ctx, receipt.ClusterID)
	if err != nil {
		return receiptReplay(params, receipt)
	}
	defer release()
	key := stringField(params["request_key"])
	receipt, err = s.readSubmission(key)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	if receipt.Result != nil || receipt.Failure != nil {
		return receiptReplay(params, receipt)
	}
	job, err := item.RecoverSubmission(ctx, receipt.Lookup)
	if err != nil {
		return nil, err
	}
	result, err := s.submissionResult(cluster, job, receipt.Revision)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	receipt.Result = result
	return s.publishSubmissionResult(key, params, receipt)
}

func (s *Service) publishSubmissionResult(key string, params map[string]any, receipt submissionReceipt) (any, error) {
	err := writeReceipt(s.submissionPath(key)+".result", receipt, true)
	if os.IsExist(err) {
		existing, readErr := s.readSubmission(key)
		if readErr != nil {
			return nil, submissionStoreError(key, readErr)
		}
		return receiptReplay(params, existing)
	}
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	return receipt.Result, nil
}
