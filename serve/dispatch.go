package serve

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/registry"
	"github.com/MolCrafts/molq/scheduler"
	"github.com/MolCrafts/molq/transport"
)

func (s *Service) ensure() {
	s.init.Do(func() {
		s.obs = NewObservation(4096)
		s.schedulers = map[string]scheduler.Scheduler{}
		s.locks = map[string]chan struct{}{}
		s.nativeSlots = make(chan struct{}, batchConcurrency)
	})
}

func (s *Service) cluster(name string) (map[string]any, scheduler.Scheduler, error) {
	s.ensure()
	s.mu.Lock()
	defer s.mu.Unlock()
	cluster, err := s.Registry.Get(name)
	if err != nil {
		return nil, nil, err
	}
	id, _ := cluster["id"].(string)
	item := s.schedulers[id]
	if item == nil {
		target := transport.Create(mapValue(cluster["transport"]))
		item, err = scheduler.Create(cluster, target)
		if err != nil {
			return nil, nil, err
		}
		s.schedulers[id] = item
	}
	return cluster, item, nil
}

func (s *Service) dispatch(method string, params map[string]any) (any, error) {
	s.ensure()
	deadline := 120 * time.Second
	if method == "files.transfer" {
		if seconds, ok := asInt(params["timeout_seconds"]); ok {
			deadline = time.Duration(seconds+5) * time.Second
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	switch method {
	case "schema.discover":
		var document any
		if err := json.Unmarshal(protocol.Current().Raw(), &document); err != nil {
			return nil, err
		}
		return document, nil
	case "clusters.list":
		return s.Registry.List()
	case "clusters.get":
		return s.Registry.Get(stringField(params["cluster"]))
	case "clusters.register", "clusters.update", "clusters.remove":
		return s.mutateCluster(method, params)
	case "jobs.validate", "jobs.preview", "jobs.submit":
		return s.submit(ctx, method, params)
	case "events.subscribe":
		return s.Since(stringField(params["cursor"])), nil
	case "jobs.get_many":
		refs := make([]map[string]any, 0)
		for _, item := range anySlice(params["refs"]) {
			refs = append(refs, mapValue(item))
		}
		return s.many(ctx, refs, stringField(params["consistency"]), params)
	case "jobs.get":
		result, err := s.many(ctx, []map[string]any{mapValue(params["ref"])}, stringField(params["consistency"]), params)
		if err != nil {
			return nil, err
		}
		errors, _ := mapValue(result["coverage"])["errors"].([]map[string]any)
		if len(errors) > 0 {
			return nil, fromData(errors[0])
		}
		return result, nil
	case "jobs.list":
		_, values, err := s.listCluster(ctx, stringField(params["cluster"]), stringField(params["project_root"]), stringField(params["project_scope"]))
		if err != nil {
			return nil, err
		}
		return Collection(filterSnapshots(values, params), nil), nil
	case "jobs.observe", "jobs.history":
		return s.query(ctx, method, params)
	case "jobs.requeue":
		return s.mutateJob(ctx, mapValue(params["ref"]), true)
	case "jobs.cancel", "jobs.cancel_many":
		return s.cancel(ctx, method, params)
	case "maintenance.inspect", "maintenance.prune":
		return s.legacyArchives(ctx, params)
	case "maintenance.prune_submissions":
		removed, retained, err := s.Registry.PruneImportedSubmissions(ctx)
		return map[string]any{"removed": removed, "retained": retained}, err
	case "logs.read":
		return s.logs(ctx, params)
	case "connections.authenticate":
		_, runner, err := s.fileBinding(stringField(params["cluster"]))
		if err != nil {
			return nil, err
		}
		ssh, ok := runner.(transport.SSH)
		if !ok {
			return map[string]any{"authenticated": true}, nil
		}
		err = ssh.Authenticate(ctx, stringField(params["code"]))
		return map[string]any{"authenticated": err == nil}, err
	case "files.read_many":
		return s.readMany(ctx, params)
	case "files.transfer":
		return s.transfer(ctx, params)
	case "files.stat", "files.list", "files.read", "files.write", "files.mkdir", "files.remove", "files.rename", "files.touch", "files.resolve", "files.chmod", "files.symlink":
		return s.files(ctx, method, params)
	case "config.reload", "config.get", "config.set", "presets.list", "presets.get", "presets.set":
		return s.configure(method, params)
	default:
		return nil, protocol.Fail("METHOD_NOT_FOUND", "Method is not implemented", map[string]any{"method": method})
	}
}

func (s *Service) mutateCluster(method string, params map[string]any) (any, error) {
	var definition map[string]any
	if method != "clusters.remove" {
		source := params
		if method == "clusters.update" {
			source = mapValue(params["definition"])
		}
		var err error
		definition, err = cleanDefinition(source)
		if err != nil {
			return nil, err
		}
	}
	var revision *int
	if method != "clusters.register" {
		value, ok := asInt(params["expected_revision"])
		if !ok {
			return nil, protocol.Fail("INVALID_INPUT", "Cluster update requires expected_revision", nil)
		}
		revision = &value
	}
	key := ""
	if method != "clusters.register" {
		key = stringField(params["cluster"])
	}
	s.ensure()
	// Definition revisions and cached Scheduler bindings change atomically.
	s.mu.Lock()
	result, err := s.Registry.Mutate(definition, key, revision)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.schedulers = map[string]scheduler.Scheduler{}
	s.mu.Unlock()
	s.obs.Clear()
	s.Notify()
	return result, nil
}

func cleanDefinition(source map[string]any) (map[string]any, error) {
	definition := mapValue(deepCopy(source))
	if definition["transport"] == nil {
		definition["transport"] = map[string]any{"kind": "local"}
	}
	if definition["defaults"] == nil {
		definition["defaults"] = map[string]any{}
	}
	delete(definition, "id")
	delete(definition, "revision")
	return definition, nil
}

func (s *Service) submit(ctx context.Context, method string, params map[string]any) (any, error) {
	key := stringField(params["request_key"])
	if method == "jobs.submit" && key != "" {
		if receipt, err := s.readSubmission(ctx, key); err == nil {
			return s.recoverSubmission(ctx, params, receipt)
		} else if !os.IsNotExist(err) {
			return nil, submissionStoreError(key, err)
		}
	}
	cluster, item, err := s.cluster(stringField(params["cluster"]))
	if err != nil {
		return nil, err
	}
	spec, err := Normalize(mapValue(params["spec"]), mapValue(cluster["defaults"]))
	if err != nil {
		return nil, err
	}
	if err := item.Validate(spec); err != nil {
		return nil, err
	}
	revision, _ := asInt(cluster["revision"])
	if method == "jobs.validate" {
		return map[string]any{"spec": spec, "cluster_revision": revision}, nil
	}
	id, _ := cluster["id"].(string)
	release, err := s.acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	defer release()
	project, err := resolveProject(ctx, cluster, stringField(params["project_root"]), stringField(params["project_scope"]))
	if err != nil {
		return nil, err
	}
	runID, err := registry.NewHex()
	if err != nil {
		return nil, err
	}
	directory := path.Join(project, scheduler.ProjectJobs, runID)
	if method == "jobs.preview" {
		script, err := item.Render(spec, directory)
		if err != nil {
			return nil, err
		}
		return map[string]any{"spec": spec, "script": script, "cluster_revision": revision}, nil
	}
	for _, edge := range anySlice(mapValue(spec["scheduling"])["dependencies"]) {
		ref := mapValue(mapValue(edge)["ref"])
		if err := s.checkRef(ref); err != nil {
			return nil, err
		}
		if ref["cluster_id"] != id {
			return nil, protocol.Fail("INVALID_INPUT", "Native dependencies must belong to the same Cluster", nil)
		}
		if _, err := s.identity(ctx, item, ref); err != nil {
			return nil, err
		}
	}
	current, err := s.Registry.Get(id)
	if err != nil {
		return nil, err
	}
	if currentRevision, _ := asInt(current["revision"]); currentRevision != revision {
		return nil, protocol.Fail("CONFLICT", "Cluster changed before native submission", nil)
	}
	if key != "" {
		return s.submitOnce(ctx, key, params, spec, cluster, item, directory, revision)
	}
	job, err := item.Submit(ctx, spec, directory)
	if err != nil {
		return nil, err
	}
	return s.submissionResult(cluster, job, revision, directory)
}

func (s *Service) submissionResult(cluster map[string]any, job scheduler.Job, revision int, directory string) (map[string]any, error) {
	project, run := scheduler.RunLocation(directory)
	if len(job.Members) == 0 {
		job.ProjectRoot, job.RunID = project, run
		ref, err := s.ref(cluster, job)
		if err != nil {
			return nil, err
		}
		value := Snapshot(ref, &job, nil)
		s.obs.Remember(value)
		return map[string]any{"ref": ref, "snapshot": value, "cluster_revision": revision, "outcome": "accepted"}, nil
	}
	refs := make([]map[string]any, 0, len(job.Members))
	snapshots := make([]map[string]any, 0, len(job.Members))
	for _, member := range job.Members {
		member.ProjectRoot, member.RunID = project, run
		ref, err := s.ref(cluster, member)
		if err != nil {
			return nil, err
		}
		value := Snapshot(ref, &member, nil)
		s.obs.Remember(value)
		refs = append(refs, ref)
		snapshots = append(snapshots, value)
	}
	return map[string]any{"array_id": job.NativeID, "refs": refs, "snapshots": snapshots, "cluster_revision": revision, "outcome": "accepted"}, nil
}

func validProjectPath(name string) bool {
	return path.IsAbs(name) && path.Clean(name) != "/" && !strings.ContainsAny(name, "\r\n\x00")
}

func resolveProject(ctx context.Context, cluster map[string]any, requested, scope string) (string, error) {
	if !validProjectPath(requested) || (scope != "" && scope != "/" && !validProjectPath(scope)) {
		return "", protocol.Fail("INVALID_INPUT", "project_root must be an absolute target project directory", nil)
	}
	runner := transport.Create(clusterTransport(cluster))
	output, err := runFileScript(ctx, runner, false, requested,
		resolvePathScript+`[ -d "$1" ] && resolve "$1" "$2"`, requested, scope)
	if err != nil {
		return "", err
	}
	resolved := strings.TrimSuffix(string(output), "\n")
	if !validProjectPath(resolved) {
		return "", protocol.Fail("FILE_UNAVAILABLE", "Cannot resolve target project directory", nil)
	}
	return resolved, nil
}

func (s *Service) ref(cluster map[string]any, job scheduler.Job) (map[string]any, error) {
	if job.Incarnation == "" {
		return nil, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Native submit identity evidence unavailable", map[string]any{"native_id": job.NativeID})
	}
	ref := map[string]any{
		"registry_id": s.Registry.ID, "cluster_id": cluster["id"],
		"native_id": job.NativeID, "incarnation": job.Incarnation,
	}
	project, run := job.ProjectRoot, job.RunID
	if job.Workdir != nil && run == "" {
		project, run = scheduler.RunLocation(*job.Workdir)
	}
	if project != "" && run != "" {
		ref["project_root"], ref["run_id"] = project, run
	}
	return ref, nil
}

func (s *Service) checkRef(ref map[string]any) error {
	if ref["registry_id"] != s.Registry.ID {
		return protocol.Fail("REGISTRY_MISMATCH", "JobRef belongs to a different registry", nil)
	}
	project, run := stringField(ref["project_root"]), stringField(ref["run_id"])
	if (project == "") != (run == "") || (project != "" && (!validProjectPath(project) || path.Clean(project) != project)) {
		return protocol.Fail("INVALID_INPUT", "JobRef project_root must be a canonical absolute target path", nil)
	}
	return nil
}

func (s *Service) identity(ctx context.Context, item scheduler.Scheduler, ref map[string]any) (scheduler.Job, error) {
	jobs, err := item.QueryMany(ctx, []string{stringField(ref["native_id"])}, stringField(ref["project_root"]))
	if err != nil {
		return scheduler.Job{}, err
	}
	job, ok := jobs[stringField(ref["native_id"])]
	if !ok || job.Incarnation == "" {
		return scheduler.Job{}, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Cannot confirm native allocation identity", nil)
	}
	if !matchesRef(job, ref) {
		return scheduler.Job{}, protocol.Fail("JOB_IDENTITY_MISMATCH", "Native ID has been reused", nil)
	}
	return job, nil
}

func matchesRef(job scheduler.Job, ref map[string]any) bool {
	if job.Incarnation != ref["incarnation"] {
		return false
	}
	if project := stringField(ref["project_root"]); project != "" && job.Workdir != nil {
		actual, run := scheduler.RunLocation(*job.Workdir)
		return actual == project && run == ref["run_id"]
	}
	return true
}

func (s *Service) logs(ctx context.Context, params map[string]any) (any, error) {
	ref := mapValue(params["ref"])
	if err := s.checkRef(ref); err != nil {
		return nil, err
	}
	cluster, item, err := s.cluster(stringField(ref["cluster_id"]))
	if err != nil {
		return nil, err
	}
	release, err := s.acquire(ctx, stringField(cluster["id"]))
	if err != nil {
		return nil, err
	}
	job, err := s.identity(ctx, item, ref)
	release()
	if err != nil {
		return nil, err
	}
	stream := stringField(params["stream"])
	if stream == "" {
		stream = "stdout"
	}
	project := stringField(ref["project_root"])
	var location string
	if project != "" && job.Workdir != nil {
		location = path.Join(*job.Workdir, stream)
	} else if stream == "stdout" && job.StdoutPath != nil {
		location = *job.StdoutPath
	} else if stream == "stderr" && job.StderrPath != nil {
		location = *job.StderrPath
	}
	if location == "" {
		if job.Workdir == nil {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Native log location unavailable", nil)
		}
		location = path.Join(*job.Workdir, stream)
	}
	offset, _ := asInt(params["offset"])
	limit, ok := asInt(params["limit"])
	if !ok {
		limit = 262144
	}
	data, err := s.readLog(ctx, cluster, location, project, offset, limit)
	return logChunk(offset, data, err)
}

func logChunk(offset int, data []byte, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}
	return map[string]any{"text": string(data), "next_offset": offset + len(data), "bytes_read": len(data)}, nil
}

func (s *Service) readLog(ctx context.Context, cluster map[string]any, location, project string, offset, limit int) ([]byte, error) {
	release, err := s.acquire(ctx, stringField(cluster["id"]))
	if err != nil {
		return nil, err
	}
	defer release()
	runner := transport.Create(clusterTransport(cluster))
	if project != "" {
		resolved, err := resolveFilePath(ctx, runner, location, project)
		if err != nil {
			return nil, err
		}
		if resolved != location {
			return nil, protocol.Fail("FILE_UNAVAILABLE", "Project log moved from its recorded location", nil)
		}
	}
	return runner.Read(ctx, location, offset, limit)
}

func nativeIDs(refs []map[string]any) []string {
	ids := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = stringField(ref["native_id"])
	}
	return ids
}

func clusterID(cluster map[string]any) any {
	if cluster == nil {
		return nil
	}
	return cluster["id"]
}

func refKeys(refs []map[string]any) any {
	if refs == nil {
		return nil
	}
	keys := make([]string, len(refs))
	for i, ref := range refs {
		keys[i] = RefKey(ref)
	}
	return keys
}

func fromData(data map[string]any) error {
	context, _ := data["context"].(map[string]any)
	outcome, _ := data["outcome"].(string)
	return protocol.FailOutcome(stringField(data["kind"]), stringField(data["message"]), outcome, context)
}

func anySlice(value any) []any {
	items, _ := value.([]any)
	return items
}
