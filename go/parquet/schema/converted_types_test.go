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

package schema_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestConvertedTypesToString(t *testing.T) {
	assert.Equal(t, "NONE", schema.ConvertedTypes.None.String())
	assert.Equal(t, "UTF8", schema.ConvertedTypes.UTF8.String())
	assert.Equal(t, "MAP", schema.ConvertedTypes.Map.String())
	assert.Equal(t, "MAP_KEY_VALUE", schema.ConvertedTypes.MapKeyValue.String())
	assert.Equal(t, "LIST", schema.ConvertedTypes.List.String())
	assert.Equal(t, "ENUM", schema.ConvertedTypes.Enum.String())
	assert.Equal(t, "DECIMAL", schema.ConvertedTypes.Decimal.String())
	assert.Equal(t, "DATE", schema.ConvertedTypes.Date.String())
	assert.Equal(t, "TIME_MILLIS", schema.ConvertedTypes.TimeMillis.String())
	assert.Equal(t, "TIME_MICROS", schema.ConvertedTypes.TimeMicros.String())
	assert.Equal(t, "TIMESTAMP_MILLIS", schema.ConvertedTypes.TimestampMillis.String())
	assert.Equal(t, "TIMESTAMP_MICROS", schema.ConvertedTypes.TimestampMicros.String())
	assert.Equal(t, "UINT_8", schema.ConvertedTypes.Uint8.String())
	assert.Equal(t, "UINT_16", schema.ConvertedTypes.Uint16.String())
	assert.Equal(t, "UINT_32", schema.ConvertedTypes.Uint32.String())
	assert.Equal(t, "UINT_64", schema.ConvertedTypes.Uint64.String())
	assert.Equal(t, "INT_8", schema.ConvertedTypes.Int8.String())
	assert.Equal(t, "INT_16", schema.ConvertedTypes.Int16.String())
	assert.Equal(t, "INT_32", schema.ConvertedTypes.Int32.String())
	assert.Equal(t, "INT_64", schema.ConvertedTypes.Int64.String())
	assert.Equal(t, "JSON", schema.ConvertedTypes.JSON.String())
	assert.Equal(t, "BSON", schema.ConvertedTypes.BSON.String())
	assert.Equal(t, "INTERVAL", schema.ConvertedTypes.Interval.String())
}
