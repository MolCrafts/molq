package serve

import (
	"context"
	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
	"sort"
	"time"
)

func (s *Service) many(ctx context.Context, refs []map[string]any, consistency string, params map[string]any) (map[string]any, error) {
	s.ensure()
	if consistency == "" {
		consistency = "live"
	}
	found := map[string]map[string]any{}
	var failures []map[string]any
	groups := map[string][]map[string]any{}
	var maxAge *time.Duration
	if consistency == "bounded" {
		age := s.cacheMaxAge(params)
		maxAge = &age
	}
	for _, ref := range refs {
		if err := s.checkRef(ref); err != nil {
			return nil, err
		}
		if consistency != "live" {
			if cached := s.obs.Cached(ref, maxAge); cached != nil {
				found[RefKey(ref)] = cached
				continue
			}
		}
		if consistency == "cached" {
			failure := protocol.Fail("CACHE_MISS", "No cached observation", nil).Data()
			failures = append(failures, failure)
			found[RefKey(ref)] = Snapshot(ref, nil, failure)
			continue
		}
		id := stringField(ref["cluster_id"])
		groups[id] = append(groups[id], ref)
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	results := make([]map[string]any, len(ids))
	runBatches(len(ids), func(i int) { results[i] = s.queryCluster(ctx, ids[i], groups[ids[i]]) })
	for _, result := range results {
		failures = append(failures, mapValue(result["coverage"])["errors"].([]map[string]any)...)
		for _, value := range result["snapshots"].([]map[string]any) {
			found[RefKey(mapValue(value["ref"]))] = value
		}
	}
	ordered := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		ordered = append(ordered, found[RefKey(ref)])
	}
	return Collection(ordered, failures), nil
}

func unavailableSnapshots(refs []map[string]any, err error) map[string]any {
	failure := asData(err)
	values := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		values = append(values, Snapshot(ref, nil, failure))
	}
	return Collection(values, []map[string]any{failure})
}

func (s *Service) queryCluster(ctx context.Context, id string, refs []map[string]any) map[string]any {
	cluster, item, err := s.cluster(id)
	if err != nil {
		return unavailableSnapshots(refs, err)
	}
	jobs, err := s.sharedQuery(ctx, cluster, item, refs)
	if err != nil {
		return unavailableSnapshots(refs, err)
	}
	values := make([]map[string]any, 0, len(refs))
	var failures []map[string]any
	var matched []scheduler.Job
	for _, ref := range refs {
		job, ok := jobs[stringField(ref["native_id"])]
		if ok && job.Incarnation != ref["incarnation"] {
			failure := protocol.Fail("JOB_IDENTITY_MISMATCH", "Native identity no longer matches", nil).Data()
			failures = append(failures, failure)
			values = append(values, Snapshot(ref, nil, failure))
			continue
		}
		var observed *scheduler.Job
		if ok {
			observed = &job
			matched = append(matched, job)
		}
		value := Snapshot(ref, observed, nil)
		values = append(values, value)
	}
	s.collectTerminal(ctx, cluster, matched)
	for _, value := range values {
		s.setCollectionState(cluster, value, jobs[stringField(mapValue(value["ref"])["native_id"])])
		s.obs.Remember(value)
	}
	return Collection(values, failures)
}

func (s *Service) listCluster(ctx context.Context, name string) (map[string]any, []map[string]any, error) {
	cluster, item, err := s.cluster(name)
	if err != nil {
		return nil, nil, err
	}
	id, _ := cluster["id"].(string)
	release, err := s.acquire(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	jobs, err := item.List(ctx)
	release()
	if err != nil {
		return nil, nil, err
	}
	values := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		if job.Incarnation == "" {
			return nil, nil, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Queue entry lacks stable native submit identity", nil)
		}
		ref, err := s.ref(cluster, job)
		if err != nil {
			return nil, nil, err
		}
		value := Snapshot(ref, &job, nil)
		values = append(values, value)
	}
	s.collectTerminal(ctx, cluster, jobs)
	for i, value := range values {
		s.setCollectionState(cluster, value, jobs[i])
		s.obs.Remember(value)
	}
	return cluster, values, nil
}

func (s *Service) query(ctx context.Context, method string, params map[string]any) (any, error) {
	if method == "jobs.history" && params["refs"] == nil {
		since, until, err := historyWindow(params)
		if err != nil {
			return nil, err
		}
		cluster, item, err := s.cluster(stringField(params["cluster"]))
		if err != nil {
			return nil, err
		}
		id, _ := cluster["id"].(string)
		release, err := s.acquire(ctx, id)
		if err != nil {
			return nil, err
		}
		jobs, err := item.HistoryRange(ctx, since, until)
		release()
		if err != nil {
			return nil, err
		}
		result := historyResult(jobs, func(job scheduler.Job) (map[string]any, error) {
			return s.ref(cluster, job)
		}, params, since, until)
		byRef := map[string]scheduler.Job{}
		for _, job := range jobs {
			if ref, err := s.ref(cluster, job); err == nil {
				byRef[RefKey(ref)] = job
			}
		}
		for _, value := range result["snapshots"].([]map[string]any) {
			job := byRef[RefKey(mapValue(value["ref"]))]
			s.rememberTerminalDirectory(ctx, cluster, job)
			s.collect(ctx, cluster, job)
			s.setCollectionState(cluster, value, job)
		}
		return result, nil
	}
	var refs []map[string]any
	if raw, ok := params["refs"].([]any); ok {
		for _, item := range raw {
			refs = append(refs, mapValue(item))
		}
	}
	var cluster map[string]any
	if name := stringField(params["cluster"]); name != "" && method == "jobs.observe" {
		var err error
		cluster, err = s.Registry.Get(name)
		if err != nil {
			return nil, err
		}
	}
	scope := mustJSON(map[string]any{"cluster": clusterID(cluster), "refs": refKeys(refs)})
	var result map[string]any
	var err error
	if refs == nil {
		if cluster == nil {
			return nil, protocol.Fail("INVALID_INPUT", "observe requires explicit refs or Cluster", nil)
		}
		result, err = s.observeCluster(ctx, cluster, string(scope), params)
	} else {
		result, err = s.many(ctx, refs, stringField(params["consistency"]), params)
	}
	if err != nil {
		return nil, err
	}
	if method != "jobs.observe" {
		return result, nil
	}
	observed := s.obs.Observe(result, string(scope), stringField(params["cursor"]))
	if changes, _ := observed["changes"].([]map[string]any); len(changes) > 0 {
		s.Notify()
	}
	return observed, nil
}

func (s *Service) observeCluster(ctx context.Context, cluster map[string]any, scope string, params map[string]any) (map[string]any, error) {
	_, values, err := s.listCluster(ctx, stringField(cluster["id"]))
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, value := range values {
		known[RefKey(mapValue(value["ref"]))] = true
	}
	var missing []map[string]any
	if cursor := stringField(params["cursor"]); cursor != "" {
		if baselineScope, items, ok := s.obs.Baseline(cursor); ok && baselineScope == scope {
			for identity, value := range items {
				if !known[identity] {
					missing = append(missing, mapValue(value["ref"]))
				}
			}
		}
	}
	if len(missing) == 0 {
		return Collection(values, nil), nil
	}
	result, err := s.many(ctx, missing, stringField(params["consistency"]), params)
	if err != nil {
		return nil, err
	}
	values = append(values, result["snapshots"].([]map[string]any)...)
	failures := mapValue(result["coverage"])["errors"].([]map[string]any)
	return Collection(values, failures), nil
}

func (s *Service) cacheMaxAge(params map[string]any) time.Duration {
	seconds := float64(5)
	if s.config != nil {
		seconds = s.config.MaxAge(seconds)
	}
	switch value := params["max_age"].(type) {
	case float64:
		seconds = value
	case int:
		seconds = float64(value)
	case int64:
		seconds = float64(value)
	}
	return time.Duration(seconds * float64(time.Second))
}
