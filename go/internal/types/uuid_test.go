package types_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.New()

func TestExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	extBuilder := array.NewExtensionBuilder(mem, types.NewUUIDType())
	defer extBuilder.Release()
	builder := types.NewUUIDBuilder(extBuilder)
	builder.Append(testUUID)
	arr := builder.NewArray()
	defer arr.Release()
	arrStr := arr.String()
	assert.Equal(t, "[\""+testUUID.String()+"\"]", arrStr)
	jsonStr, err := json.Marshal(arr)
	assert.NoError(t, err)

	arr1, _, err := array.FromJSON(mem, types.NewUUIDType(), bytes.NewReader(jsonStr))
	defer arr1.Release()
	assert.NoError(t, err)
	assert.Equal(t, arr, arr1)
}

func TestExtensionRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: types.NewUUIDType()},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*types.UUIDBuilder).Append(testUUID)
	record := builder.NewRecord()
	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "[{\"uuid\":\""+testUUID.String()+"\"}\n]", string(b))
	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, record, record1)
}
