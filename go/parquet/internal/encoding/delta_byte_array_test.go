package encoding

import (
	"fmt"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDeltaByteArrayDecoder_SetData(t *testing.T) {
	tests := []struct {
		name    string
		nvalues int
		data    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "null only page",
			nvalues: 126609,
			data:    []byte{128, 1, 4, 0, 0},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		d := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, nil, memory.DefaultAllocator)
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, d.SetData(tt.nvalues, tt.data), fmt.Sprintf("SetData(%v, %v)", tt.nvalues, tt.data))
		})
	}
}
