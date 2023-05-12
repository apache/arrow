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
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestListOf(t *testing.T) {
	n := schema.NewByteArrayNode("str", parquet.Repetitions.Required, 1)
	list, err := schema.ListOf(n, parquet.Repetitions.Optional, 2)

	assert.NoError(t, err)
	assert.Equal(t, "str", list.Name())
	assert.Equal(t, parquet.Repetitions.Optional, list.RepetitionType())
	assert.Equal(t, 1, list.NumFields())
	assert.EqualValues(t, 2, list.FieldID())
	assert.IsType(t, &schema.GroupNode{}, list.Field(0))
	assert.Equal(t, "list", list.Field(0).Name())
	assert.Equal(t, 1, list.Field(0).(*schema.GroupNode).NumFields())
	assert.Same(t, n, list.Field(0).(*schema.GroupNode).Field(0))
	assert.Equal(t, "element", list.Field(0).(*schema.GroupNode).Field(0).Name())
}

func TestListOfNested(t *testing.T) {
	n, err := schema.ListOf(schema.NewInt32Node("arrays", parquet.Repetitions.Required, -1), parquet.Repetitions.Required, -1)
	assert.NoError(t, err)
	final, err := schema.ListOf(n, parquet.Repetitions.Required, -1)
	assert.NoError(t, err)

	var buf bytes.Buffer
	schema.PrintSchema(final, &buf, 4)
	assert.Equal(t,
		`required group field_id=-1 arrays (List) {
    repeated group field_id=-1 list {
        required group field_id=-1 element (List) {
            repeated group field_id=-1 list {
                required int32 field_id=-1 element;
            }
        }
    }
}`, strings.TrimSpace(buf.String()))
}

func TestMapOfNestedTypes(t *testing.T) {
	n, err := schema.NewGroupNode("student", parquet.Repetitions.Required, schema.FieldList{
		schema.NewByteArrayNode("name", parquet.Repetitions.Required, -1),
		schema.NewInt32Node("age", parquet.Repetitions.Optional, -1),
	}, -1)
	assert.NoError(t, err)

	grp, err := schema.NewGroupNode("classes", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewInt32Node("a", parquet.Repetitions.Repeated, -1),
		schema.NewFloat32Node("b", parquet.Repetitions.Repeated, -1),
	}, -1)
	assert.NoError(t, err)

	classes, err := schema.ListOf(grp, parquet.Repetitions.Optional, -1)
	assert.NoError(t, err)

	m, err := schema.MapOf("studentmap", n, classes, parquet.Repetitions.Required, 1)
	assert.NoError(t, err)

	var buf bytes.Buffer
	schema.PrintSchema(m, &buf, 4)
	assert.Equal(t,
		`required group field_id=1 studentmap (Map) {
    repeated group field_id=-1 key_value {
        required group field_id=-1 key {
            required byte_array field_id=-1 name;
            optional int32 field_id=-1 age;
        }
        optional group field_id=-1 value (List) {
            repeated group field_id=-1 list {
                optional group field_id=-1 element {
                    repeated int32 field_id=-1 a;
                    repeated float field_id=-1 b;
                }
            }
        }
    }
}`, strings.TrimSpace(buf.String()))
}
