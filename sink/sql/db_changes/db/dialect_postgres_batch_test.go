package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

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
