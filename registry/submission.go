package registry

import (
	"context"
	"database/sql"
	"errors"
	"os"
)

const submissionTable = `CREATE TABLE submission_operations (
	key_digest TEXT PRIMARY KEY NOT NULL,
	record TEXT NOT NULL,
	final INTEGER NOT NULL CHECK(final IN (0,1)),
	legacy INTEGER NOT NULL CHECK(legacy IN (0,1))
)`

// ReadSubmission returns the durable evidence for a request key digest.
func (r *Registry) ReadSubmission(ctx context.Context, digest string) ([]byte, bool, error) {
	var body []byte
	var legacy bool
	err := r.db.QueryRowContext(ctx, "SELECT record, legacy FROM submission_operations WHERE key_digest=?", digest).Scan(&body, &legacy)
	if errors.Is(err, sql.ErrNoRows) {
		err = os.ErrNotExist
	}
	return body, legacy, err
}

// ClaimSubmission durably reserves a digest before dispatching a native command.
func (r *Registry) ClaimSubmission(ctx context.Context, digest string, body []byte) (bool, error) {
	result, err := r.db.ExecContext(ctx, `INSERT INTO submission_operations VALUES (?, ?, 0, 0)
		ON CONFLICT(key_digest) DO NOTHING`, digest, string(body))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

// UpdateSubmission adds evidence to an unresolved operation. An established final
// receipt is immutable, even if another serve process finishes reconciling it.
func (r *Registry) UpdateSubmission(ctx context.Context, digest string, body []byte, final bool) error {
	_, err := r.db.ExecContext(ctx, `UPDATE submission_operations SET record=?, final=?
		WHERE key_digest=? AND final=0 AND legacy=0`, string(body), final, digest)
	return err
}
