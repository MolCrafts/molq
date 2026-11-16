package serve

import (
	"context"
	"sort"
)

func (s *Service) cancel(ctx context.Context, method string, params map[string]any) (any, error) {
	refs := []map[string]any{}
	if method == "jobs.cancel_many" {
		for _, item := range anySlice(params["refs"]) {
			refs = append(refs, mapValue(item))
		}
	} else {
		refs = append(refs, mapValue(params["ref"]))
	}
	if method == "jobs.cancel" {
		outcome, err := s.mutateJob(ctx, refs[0], false)
		if err != nil {
			return nil, err
		}
		outcome["ref"] = refs[0]
		return outcome, nil
	}
	groups := map[string][]int{}
	for i, ref := range refs {
		id := stringField(ref["cluster_id"])
		groups[id] = append(groups[id], i)
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	results := make([]map[string]any, len(refs))
	runBatches(len(ids), func(i int) {
		for _, index := range groups[ids[i]] {
			ref := refs[index]
			outcome, err := s.mutateJob(ctx, ref, false)
			if err != nil {
				results[index] = map[string]any{"ref": ref, "error": asData(err)}
				continue
			}
			outcome["ref"] = ref
			results[index] = outcome
		}
	})
	partial := false
	for _, result := range results {
		if result["error"] != nil {
			partial = true
		}
	}
	return map[string]any{"results": results, "partial_failure": partial}, nil
}

func (s *Service) mutateJob(ctx context.Context, ref map[string]any, requeue bool) (map[string]any, error) {
	if err := s.checkRef(ref); err != nil {
		return nil, err
	}
	_, item, err := s.cluster(stringField(ref["cluster_id"]))
	if err != nil {
		return nil, err
	}
	id := stringField(ref["cluster_id"])
	release, err := s.acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	defer release()
	job, err := s.identity(ctx, item, ref)
	if err != nil {
		return nil, err
	}
	if requeue {
		if err := item.Requeue(ctx, job); err != nil {
			return nil, err
		}
		return map[string]any{"outcome": "accepted", "ref": ref}, nil
	}
	return item.Cancel(ctx, job)
}
