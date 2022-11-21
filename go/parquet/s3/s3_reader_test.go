package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutor(t *testing.T) {
	// Uncomment this line to test locally.
	// t.Skip("Skipping cluster test in github CI.")

	_, err := OpenParquetFile("test/foobar.parquet")

	// require.Equal(t, , "table", res)
	require.NoError(t, err)
}
