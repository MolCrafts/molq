package scheduler

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// Minimal sanitized squeue response captured on Arrhenius, Slurm 25.05.8.
const slurmEmptyQueue = `{"jobs":[],"errors":[],"warnings":[{"description":"Zero jobs to dump","source":""}]}`

func TestSlurmEmptyQueueWarningQueriesAccounting(t *testing.T) {
	fake := script(slurmEmptyQueue,
		`{"jobs":[{"job_id":"42","state":{"current":["COMPLETED"]},"time":{"submission":1710000000,"end":1710000010},"exit_code":{"return_code":{"set":true,"infinite":false,"number":0},"signal":{"id":{"set":true,"infinite":false,"number":0}}}}],"errors":[],"warnings":[]}`)
	jobs, err := NewSlurm(fake, map[string]any{}).QueryMany(context.Background(), []string{"42"})
	if err != nil {
		t.Fatal(err)
	}
	job := jobs["42"]
	if job.State != "succeeded" || job.Source != "accounting" || job.EndedAt == nil || job.ExitCode == nil || *job.ExitCode != 0 {
		t.Fatalf("job=%+v", job)
	}
	want := [][]string{
		{"squeue", "--array", "--json", "--jobs", "42"},
		{"sacct", "--allocations", "--json", "--duplicates", "--jobs", "42"},
	}
	if !reflect.DeepEqual(fake.calls, want) {
		t.Fatalf("calls=%v", fake.calls)
	}
}

func TestSlurmListEmptyQueueWarning(t *testing.T) {
	fake := script(slurmEmptyQueue)
	jobs, err := NewSlurm(fake, map[string]any{}).List(context.Background())
	if err != nil || jobs == nil || len(jobs) != 0 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	if !reflect.DeepEqual(fake.calls, [][]string{{"squeue", "--me", "--array", "--json"}}) {
		t.Fatalf("calls=%v", fake.calls)
	}
}

func TestSlurmEmptyQueueWarningDoesNotHidePartialResponses(t *testing.T) {
	for _, test := range []struct {
		name, source string
		change       func(map[string]any)
	}{
		{"accounting", "accounting", func(map[string]any) {}},
		{"nonempty_queue", "queue", func(data map[string]any) {
			data["jobs"] = []any{map[string]any{"job_id": "42", "job_state": "RUNNING", "submit_time": 1710000000}}
		}},
		{"errors", "queue", func(data map[string]any) {
			data["errors"] = []any{map[string]any{"description": "Access denied"}}
		}},
		{"missing_jobs", "queue", func(data map[string]any) { delete(data, "jobs") }},
		{"null_jobs", "queue", func(data map[string]any) { data["jobs"] = nil }},
		{"unknown_warning", "queue", func(data map[string]any) {
			data["warnings"] = []any{map[string]any{"description": "Incomplete query", "source": ""}}
		}},
		{"additional_warning", "queue", func(data map[string]any) {
			data["warnings"] = append(data["warnings"].([]any), map[string]any{"description": "Incomplete query", "source": ""})
		}},
		{"unexpected_source", "queue", func(data map[string]any) {
			data["warnings"].([]any)[0].(map[string]any)["source"] = "slurmdbd"
		}},
		{"missing_source", "queue", func(data map[string]any) {
			delete(data["warnings"].([]any)[0].(map[string]any), "source")
		}},
		{"additional_warning_field", "queue", func(data map[string]any) {
			data["warnings"].([]any)[0].(map[string]any)["error_number"] = 1
		}},
		{"malformed_warning", "queue", func(data map[string]any) { data["warnings"] = "Zero jobs to dump" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var data map[string]any
			if err := json.Unmarshal([]byte(slurmEmptyQueue), &data); err != nil {
				t.Fatal(err)
			}
			test.change(data)
			body, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			_, err = parseSlurmJobs(string(body), test.source)
			failure, ok := err.(*protocol.Error)
			if !ok || failure.Kind != "SCHEDULER_UNAVAILABLE" {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestSlurmGroupedPendingArrayAndAccountingIdentity(t *testing.T) {
	fake := script(
		`{"jobs":[{"job_id":42,"array_job_id":{"set":true,"number":42},"array_task_id":{"set":false,"number":0},"array_task_string":"0,2-4:2","job_state":["PENDING"],"submit_time":1710000000,"current_working_directory":"/target/job"}]}`,
		`{"jobs":[{"job_id":43,"array":{"job_id":42,"task_id":{"set":true,"number":1}},"state":{"current":["OUT_OF_MEMORY"]},"time":{"submission":1710000000,"end":1710000010},"working_directory":"/target/job"}]}`,
	)
	jobs, err := NewSlurm(fake, nil).QueryMany(context.Background(), []string{"42_0", "42_1", "42_2", "42_4"})
	if err != nil || len(jobs) != 4 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	if job := jobs["42_1"]; job.FailureReason != "out_of_memory" || job.Workdir == nil || *job.Workdir != "/target/job/1" {
		t.Fatalf("accounting array member = %+v", job)
	}
	if job := jobs["42_0"]; job.State != "queued" || job.Incarnation != jobs["42_1"].Incarnation {
		t.Fatalf("queue array member = %+v", job)
	}
}

func TestSlurmRecoveryMatchesUniqueWorkdirAcrossQueueAndAccounting(t *testing.T) {
	fake := script(
		`{"jobs":[]}`,
		`{"jobs":[{"job_id":42,"state":{"current":["COMPLETED"]},"time":{"submission":1710000000,"end":1710000010},"working_directory":"/target/job"},{"job_id":43,"state":{"current":["COMPLETED"]},"time":{"submission":1710000000,"end":1710000010},"working_directory":"/target/other"}]}`,
	)
	job, err := NewSlurm(fake, nil).RecoverSubmission(context.Background(), SubmissionLookup{Directory: "/target/job", Since: time.Unix(1710000000, 0)})
	if err != nil || job.NativeID != "42" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}
