package scheduler

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// SubmissionLookup identifies one previously dispatched submission.
type SubmissionLookup struct {
	Directory string
	NativeID  string
	Name      string
	Since     time.Time
	Indices   []int
}

func submissionUnknown(id string) error {
	return protocol.FailOutcome("OUTCOME_UNKNOWN", "Native submission evidence is incomplete or ambiguous", "unknown", map[string]any{"native_id": id})
}

func (c *core) RecoverSubmission(_ context.Context, lookup SubmissionLookup) (Job, error) {
	return Job{}, submissionUnknown(lookup.NativeID)
}

func (s *Shell) RecoverSubmission(ctx context.Context, lookup SubmissionLookup) (Job, error) {
	id := path.Base(lookup.Directory)
	jobs, err := s.QueryMany(ctx, []string{id})
	if err == nil && jobs[id].Incarnation != "" {
		return jobs[id], nil
	}
	return Job{}, submissionUnknown(id)
}

func (s *Slurm) RecoverSubmission(ctx context.Context, lookup SubmissionLookup) (Job, error) {
	if lookup.NativeID != "" {
		if len(lookup.Indices) > 0 {
			job, err := s.acceptedArray(ctx, lookup.NativeID, lookup.Indices)
			if err == nil && matchingSubmission(job.Members, lookup) {
				return job, nil
			}
		} else {
			jobs, err := s.QueryMany(ctx, []string{lookup.NativeID})
			job := jobs[lookup.NativeID]
			if err == nil && matchingSubmission([]Job{job}, lookup) {
				return job, nil
			}
		}
		return Job{}, submissionUnknown(lookup.NativeID)
	}
	name := lookup.Name
	if name == "" {
		name = "job.sh"
	}
	queue, err := s.native(ctx, []string{"squeue", "--me", "--array", "--json", "--name=" + name}, false, nil, false)
	if err != nil {
		return Job{}, submissionUnknown("")
	}
	live, err := parseSlurmJobs(queue, "queue")
	if err != nil {
		return Job{}, submissionUnknown("")
	}
	start := lookup.Since.Add(-time.Minute).UTC().Format("2006-01-02T15:04:05")
	command := "exec env TZ=UTC " + joinStrings([]string{"sacct", "--allocations", "--json", "--duplicates", "--starttime=" + start, "--name=" + name}) + ` --user="$(id -un)"`
	text, err := s.native(ctx, []string{"sh", "-c", command}, false, nil, false)
	if err != nil {
		return Job{}, submissionUnknown("")
	}
	history, err := parseSlurmJobs(text, "accounting")
	if err != nil {
		return Job{}, submissionUnknown("")
	}
	found := map[string]Job{}
	for _, job := range append(history, live...) {
		if !matchesSubmissionJob(job, lookup) {
			continue
		}
		if prev, ok := found[job.NativeID]; ok && prev.Incarnation != job.Incarnation {
			return Job{}, submissionUnknown("")
		}
		found[job.NativeID] = job
	}
	if len(lookup.Indices) == 0 && len(found) == 1 {
		for _, job := range found {
			return job, nil
		}
	}
	if len(lookup.Indices) != 0 && len(found) == len(lookup.Indices) {
		var parent string
		for id := range found {
			current, _, _ := strings.Cut(id, "_")
			if parent != "" && parent != current {
				return Job{}, submissionUnknown("")
			}
			parent = current
		}
		members := make([]Job, 0, len(found))
		for _, index := range lookup.Indices {
			if member, ok := found[parent+"_"+itoa(index)]; ok {
				members = append(members, member)
			}
		}
		if matchingSubmission(members, lookup) {
			return Job{NativeID: parent, Members: members}, nil
		}
	}
	return Job{}, submissionUnknown("")
}

func matchingSubmission(jobs []Job, lookup SubmissionLookup) bool {
	if len(jobs) != max(1, len(lookup.Indices)) {
		return false
	}
	for _, job := range jobs {
		if !matchesSubmissionJob(job, lookup) {
			return false
		}
	}
	return true
}

func matchesSubmissionJob(job Job, lookup SubmissionLookup) bool {
	if job.Workdir == nil || job.SubmittedAt == nil || job.Incarnation == "" {
		return false
	}
	submitted, err := time.Parse(time.RFC3339, *job.SubmittedAt)
	if err != nil || submitted.Before(lookup.Since.Add(-time.Minute)) {
		return false
	}
	_, index, array := strings.Cut(job.NativeID, "_")
	if len(lookup.Indices) == 0 {
		return !array && *job.Workdir == lookup.Directory
	}
	return array && *job.Workdir == path.Join(lookup.Directory, index)
}
