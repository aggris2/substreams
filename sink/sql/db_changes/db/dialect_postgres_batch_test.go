package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingTx records each query as sent, to see how statements are split in round trips.
type countingTx struct {
	TestTx
	sent []string
}

func (t *countingTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	t.sent = append(t.sent, query)
	return &testResult{rowsAffected: 1}, nil
}

func TestPostgresStatementBatch(t *testing.T) {
	ctx := context.Background()
	op := &Operation{}

	t.Run("splits by statement count, in order", func(t *testing.T) {
		tx := &countingTx{}
		batch := postgresStatementBatch{tx: tx}
		total := postgresBatchStatements + 5
		for i := range total {
			require.NoError(t, batch.add(ctx, op, fmt.Sprintf("SELECT %d", i)))
		}
		require.NoError(t, batch.flush(ctx))

		require.Len(t, tx.sent, 2)
		assert.Equal(t, postgresBatchStatements, strings.Count(tx.sent[0], ";\n"))
		assert.Equal(t, 5, strings.Count(tx.sent[1], ";\n"))
		assert.True(t, strings.HasPrefix(tx.sent[0], "SELECT 0;\nSELECT 1;\n"))
		assert.True(t, strings.HasPrefix(tx.sent[1], fmt.Sprintf("SELECT %d;\n", postgresBatchStatements)))
	})

	t.Run("splits by size", func(t *testing.T) {
		tx := &countingTx{}
		batch := postgresStatementBatch{tx: tx}
		require.NoError(t, batch.add(ctx, op, strings.Repeat("x", postgresBatchBytes)))
		assert.Len(t, tx.sent, 1)
	})

	t.Run("keeps a statement's own semicolon, and sends nothing when empty", func(t *testing.T) {
		tx := &countingTx{}
		batch := postgresStatementBatch{tx: tx}
		require.NoError(t, batch.flush(ctx))
		assert.Empty(t, tx.sent)

		require.NoError(t, batch.add(ctx, op, "SELECT 1;"))
		require.NoError(t, batch.flush(ctx))
		assert.Equal(t, []string{"SELECT 1;\n"}, tx.sent)
	})
}

func TestPostgresFlush_ErrorSaysWhatFailed(t *testing.T) {
	ctx := context.Background()
	schema := pgTestSchema(t)
	l := pgTestLoader(t, schema, true)
	require.NoError(t, l.InsertCursor(ctx, testModuleHash, pgCursor(1, 1)))

	require.NoError(t, l.Insert("acc", map[string]string{"id": "taken"}, map[string]FieldData{"n": {Value: "1"}}, nil))
	_, err := l.Flush(ctx, testModuleHash, pgCursor(2, 2), 2)
	require.NoError(t, err)

	// A failed flush keeps its pending changes: each case starts from a new loader.
	loaderWithRows := func() *Loader {
		l := pgTestLoader(t, schema, true)
		for i := range postgresBatchStatements + 10 {
			require.NoError(t, l.Insert("acc", map[string]string{"id": fmt.Sprintf("r%d", i)}, map[string]FieldData{"n": {Value: "1"}}, nil))
		}
		return l
	}
	countRows := func() (count int) {
		require.NoError(t, l.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.acc`, EscapeIdentifier(l.dsn.Schema()))).Scan(&count))
		return count
	}

	t.Run("a constraint names the key", func(t *testing.T) {
		l := loaderWithRows()
		require.NoError(t, l.Insert("acc", map[string]string{"id": "taken"}, map[string]FieldData{"n": {Value: "2"}}, nil))
		_, err := l.Flush(ctx, testModuleHash, pgCursor(3, 3), 3)
		require.ErrorContains(t, err, "Key (id)=(taken) already exists")
		require.ErrorContains(t, err, "duplicate key")
		assert.Equal(t, 1, countRows(), "nothing of the flush stays")
	})

	t.Run("an invalid value names its statement", func(t *testing.T) {
		l := loaderWithRows()
		require.NoError(t, l.Insert("acc", map[string]string{"id": "bad"}, map[string]FieldData{"n": {Value: "not-a-number"}}, nil))
		_, err := l.Flush(ctx, testModuleHash, pgCursor(3, 3), 3)
		require.ErrorContains(t, err, "at statement")
		require.ErrorContains(t, err, "not-a-number")
		assert.NotContains(t, err.Error(), "'r0'", "only the failing statement")
		assert.Equal(t, 1, countRows(), "nothing of the flush stays")
	})
}

func TestPostgresStatementBatch_StatementAt(t *testing.T) {
	ctx := context.Background()
	tx := &failingTx{err: &pq.Error{Message: "invalid input", Position: "31"}}
	batch := postgresStatementBatch{tx: tx}
	op := &Operation{}
	require.NoError(t, batch.add(ctx, op, "SELECT 'é';"))
	require.NoError(t, batch.add(ctx, op, "INSERT INTO t VALUES ('bad');"))
	require.NoError(t, batch.add(ctx, op, "SELECT 3;"))

	err := batch.flush(ctx)
	require.ErrorContains(t, err, `at statement "INSERT INTO t VALUES ('bad');"`)
	require.ErrorContains(t, err, "invalid input")

	assert.Empty(t, batch.statementAt(""))
	assert.Equal(t, "SELECT 'é';", batch.statementAt("1"))
	assert.Equal(t, "SELECT 3;", batch.statementAt("44"))
}

type failingTx struct {
	countingTx
	err error
}

func (t *failingTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, t.err
}

// BenchmarkPostgresFlush compares a flush sent one statement per round trip with the same flush
// sent in batches. The gain grows with the latency to the database, so point PG_DSN at a remote
// one to see it over a network:
//
//	PG_DSN="psql://user:pass@host:5432/db?sslmode=disable" go test ./sink/sql/db_changes/db/ -run '^$' -bench BenchmarkPostgresFlush
func BenchmarkPostgresFlush(b *testing.B) {
	const rows = 1000
	defer func(previous int) { postgresBatchStatements = previous }(postgresBatchStatements)

	for _, statements := range []int{1, postgresBatchStatements} {
		b.Run(fmt.Sprintf("statements_per_round_trip=%d", statements), func(b *testing.B) {
			postgresBatchStatements = statements
			ctx := context.Background()
			l := pgTestLoader(b, pgTestSchema(b), true)
			require.NoError(b, l.InsertCursor(ctx, testModuleHash, pgCursor(1, 1)))

			block := uint64(1)
			for b.Loop() {
				b.StopTimer()
				block++
				for i := range rows {
					id := map[string]string{"id": fmt.Sprintf("%d-%d", block, i)}
					require.NoError(b, l.Insert("acc", id, map[string]FieldData{"n": {Value: "1"}}, nil))
				}
				b.StartTimer()
				_, err := l.Flush(ctx, testModuleHash, pgCursor(block, block), block)
				require.NoError(b, err)
			}
			b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
