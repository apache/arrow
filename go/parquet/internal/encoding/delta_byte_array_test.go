// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"fmt"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
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
