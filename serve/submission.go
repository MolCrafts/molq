package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

type submissionReceipt struct {
	Fingerprint string
	ClusterID   string
	Revision    int
	Binding     string
	Lookup      scheduler.SubmissionLookup
	Result      map[string]any
	Failure     map[string]any
}

func submissionDigest(key string) string {
	return fingerprint("request_key", map[string]any{"key": key})
}

func (s *Service) readSubmission(ctx context.Context, key string) (submissionReceipt, error) {
	body, legacy, err := s.Registry.ReadSubmission(ctx, submissionDigest(key))
	var receipt submissionReceipt
	if err != nil {
		return receipt, err
	}
	if legacy {
		return receipt, protocol.Fail("CONFLICT", "request_key belongs to a submission made before the project layout upgrade", nil)
	}
	if err := json.Unmarshal(body, &receipt); err != nil {
		return receipt, err
	}
	if receipt.Fingerprint == "" || receipt.Binding == "" || receipt.ClusterID == "" || receipt.Lookup.Since.IsZero() {
		return receipt, fmt.Errorf("incomplete submission evidence")
	}
	return receipt, nil
}

func submissionStoreError(key string, err error) error {
	if item, ok := err.(*protocol.Error); ok && item.Kind == "CONFLICT" {
		return item
	}
	return protocol.FailOutcome("OUTCOME_UNKNOWN", "Cannot verify submission receipt", "unknown", map[string]any{"request_key": key, "detail": err.Error()})
}

func (s *Service) submitOnce(ctx context.Context, key string, params, spec, cluster map[string]any, item scheduler.Scheduler, directory string, revision int) (any, error) {
	scheduling := mapValue(spec["scheduling"])
	receipt := submissionReceipt{
		Fingerprint: fingerprint("jobs.submit", params),
		ClusterID:   stringField(cluster["id"]), Revision: revision,
		Binding: fingerprint("submission_binding", map[string]any{"scheduler": cluster["scheduler"], "transport": clusterTransport(cluster)}),
		Lookup:  scheduler.SubmissionLookup{Directory: directory, Since: time.Now().UTC(), Name: stringField(scheduling["name"])},
	}
	for _, raw := range anySlice(mapValue(scheduling["array"])["indices"]) {
		index, _ := asInt(raw)
		receipt.Lookup.Indices = append(receipt.Lookup.Indices, index)
	}
	sort.Ints(receipt.Lookup.Indices)
	body, err := json.Marshal(receipt)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	claimed, err := s.Registry.ClaimSubmission(ctx, submissionDigest(key), body)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	if !claimed {
		existing, err := s.readSubmission(ctx, key)
		if err != nil {
			return nil, submissionStoreError(key, err)
		}
		return receiptReplay(params, existing)
	}
	job, err := item.Submit(ctx, spec, directory)
	if err != nil {
		if failure, ok := err.(*protocol.Error); ok {
			receipt.Lookup.NativeID = stringField(failure.Context["native_id"])
			if failure.Outcome == "not_applied" {
				receipt.Failure = failure.Data()
			}
		}
		if receipt.Failure != nil || receipt.Lookup.NativeID != "" {
			if writeErr := s.updateSubmission(ctx, key, receipt, receipt.Failure != nil); writeErr != nil {
				return nil, submissionStoreError(key, writeErr)
			}
		}
		return nil, err
	}
	result, err := s.submissionResult(cluster, job, revision, directory)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	receipt.Result = result
	return s.publishSubmissionResult(ctx, key, params, receipt)
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
	if err != nil || fingerprint("submission_binding", map[string]any{"scheduler": cluster["scheduler"], "transport": clusterTransport(cluster)}) != receipt.Binding {
		return receiptReplay(params, receipt)
	}
	release, err := s.acquire(ctx, receipt.ClusterID)
	if err != nil {
		return receiptReplay(params, receipt)
	}
	defer release()
	key := stringField(params["request_key"])
	receipt, err = s.readSubmission(ctx, key)
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
	result, err := s.submissionResult(cluster, job, receipt.Revision, receipt.Lookup.Directory)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	receipt.Result = result
	return s.publishSubmissionResult(ctx, key, params, receipt)
}

func (s *Service) updateSubmission(ctx context.Context, key string, receipt submissionReceipt, final bool) error {
	body, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return s.Registry.UpdateSubmission(storeCtx, submissionDigest(key), body, final)
}

func (s *Service) publishSubmissionResult(ctx context.Context, key string, params map[string]any, receipt submissionReceipt) (any, error) {
	if err := s.updateSubmission(ctx, key, receipt, true); err != nil {
		return nil, submissionStoreError(key, err)
	}
	existing, err := s.readSubmission(context.WithoutCancel(ctx), key)
	if err != nil {
		return nil, submissionStoreError(key, err)
	}
	return receiptReplay(params, existing)
}
