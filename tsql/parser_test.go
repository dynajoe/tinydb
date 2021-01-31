package tsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			name: "select star",
			text: "SELECT * FROM foo",
		},
		{
			name: "select columns",
			text: "SELECT a, b FROM foo",
		},
		{
			name: "select cross join",
			text: "SELECT a, b FROM foo, bar",
		},
		{
			name: "select with where clause",
			text: "SELECT a, b FROM foo, bar WHERE a = 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := require.New(t)
			selectStatement, err := Parse(tc.text)
			assert.NoError(err)
			assert.NotNil(selectStatement)
		})
	}
}
