// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dataset_test

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/dataset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getBlogTestSchema() *arrow.Schema {
	blogMeta := arrow.NewMetadata([]string{"parquet.proto.descriptor", "writer.model.name", "parquet.proto.class"},
		[]string{"name: \"Blog\"\nfield {\n  name: \"reply\"\n  number: 1\n  label: LABEL_OPTIONAL\n  type: TYPE_MESSAGE\n  type_name: \".org.apache.arrow.rust.example.Reply\"\n}\nfield {\n  name: \"blog_id\"\n  number: 2\n  label: LABEL_OPTIONAL\n  type: TYPE_INT64\n}\n",
			"protobuf", "org.apache.arrow.rust.example.Blogs$Blog"})
	return arrow.NewSchema([]arrow.Field{
		{
			Name: "reply", Type: arrow.StructOf(
				arrow.Field{Name: "reply_id", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})},
				arrow.Field{Name: "next_id", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})},
			), Nullable: true, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"}),
		},
		{
			Name: "blog_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"}),
		},
	}, &blogMeta)
}

func TestSimple(t *testing.T) {
	pwd, _ := filepath.Abs("../../")
	factory, err := dataset.CreateDatasetFactory("file://"+filepath.Join(pwd, "testing/data/parquet/generated_simple_numerics/blogs.parquet"), dataset.FileFormat(dataset.PARQUET))
	assert.NoError(t, err)

	sc, err := factory.Inspect(dataset.DefaultInspectFragments)
	assert.NoError(t, err)

	expected := getBlogTestSchema()
	assert.True(t, expected.Equal(sc))
}

func TestReader(t *testing.T) {
	pwd, _ := filepath.Abs("../../")
	dir := filepath.Join(pwd, "testing/data/parquet/generated_simple_numerics")

	f, err := os.Open(filepath.Join(dir, "blogs.json"))
	require.NoError(t, err)
	defer f.Close()

	type data struct {
		Reply *struct {
			ReplyID int32  `json:"reply_id"`
			NextID  *int32 `json:"next_id,omitempty"`
		} `json:"reply,omitempty"`
		BlogID *int64 `json:"blog_id,omitempty"`
	}

	var contents []data
	require.NoError(t, json.NewDecoder(f).Decode(&contents))

	factory, err := dataset.CreateDatasetFactory("file://"+filepath.Join(dir, "blogs.parquet"), dataset.FileFormat(dataset.PARQUET))
	require.NoError(t, err)

	ds, err := factory.CreateDataset()
	require.NoError(t, err)

	scanner, err := ds.NewScan([]string{"reply", "blog_id"}, 8192)
	require.NoError(t, err)

	rdr, err := scanner.GetReader()
	require.NoError(t, err)

	idx := 0
	for {
		rec, err := rdr.Read()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		for i := 0; i < int(rec.NumRows()); i, idx = i+1, idx+1 {
			if contents[idx].Reply == nil {
				assert.True(t, rec.Column(0).IsNull(i))
			} else {
				st := rec.Column(0).(*array.Struct)
				assert.Equal(t, contents[idx].Reply.ReplyID, st.Field(0).(*array.Int32).Value(i))
				if contents[idx].Reply.NextID == nil {
					assert.True(t, st.Field(1).IsNull(i))
				} else {
					assert.Equal(t, *contents[idx].Reply.NextID, st.Field(1).(*array.Int32).Value(i))
				}
			}

			if contents[idx].BlogID == nil {
				assert.True(t, rec.Column(1).IsNull(i))
			} else {
				assert.Equal(t, *contents[idx].BlogID, rec.Column(1).(*array.Int64).Value(i))
			}
		}
	}
}
