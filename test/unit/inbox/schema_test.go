package inbox_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/inbox"
)

// ApplySchema validates its runner before touching a database.
func TestApplySchema_ValidatesRunner(t *testing.T) {
	require.ErrorContains(t, inbox.ApplySchema(context.Background(), &migrate.Runner{}), "non-empty DSN")
	require.ErrorContains(t, inbox.ApplySchema(context.Background(), &migrate.Runner{DSN: "postgres://%zz"}),
		"could not set migrations table")
}
