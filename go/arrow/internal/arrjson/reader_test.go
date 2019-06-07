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

package arrjson // import "github.com/apache/arrow/go/arrow/internal/arrjson"

import (
	"io"
	"log"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestReader(t *testing.T) {
	for _, tc := range []struct {
		name   string
		json   string
		schema *arrow.Schema
		want   []array.Record
	}{
		{
			name: "example1",
			json: example1,
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "foo", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			}, nil),
			want: nil,
		},
		{
			name: "example2",
			json: example2,
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "foo", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "bar", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			}, nil),
			want: nil,
		},
		{
			name: "example3",
			json: example3,
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "list_nullable", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				{Name: "struct_nullable", Type: arrow.StructOf([]arrow.Field{
					{Name: "f1", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					{Name: "f2", Type: arrow.BinaryTypes.String, Nullable: true},
				}...), Nullable: true},
			}, nil),
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			r, err := NewReader(strings.NewReader(tc.json), WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()

			if got, want := r.Schema(), tc.schema; !got.Equal(want) {
				t.Fatalf("invalid schema\ngot:\n%v\nwant:\n%v\n", got, want)
			}
			for {
				rec, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				log.Printf("record:")
				for j, col := range rec.Columns() {
					log.Printf("col[%d][%s]: %v\n", j, rec.Schema().Field(j).Name, col)
				}
			}
		})
	}
}

const (
	example1 = `{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 32},
        "nullable": true, "children": [],
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 32}
          ]
        }
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "foo",
          "count": 5,
          "DATA": [1, 2, 3, 4, 5],
          "VALIDITY": [1, 0, 1, 1, 1]
        }
      ]
    }
  ]
}`
	example2 = `{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 32},
        "nullable": true, "children": [],
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 32}
          ]
        }
      },
      {
        "name": "bar",
        "type": {"name": "floatingpoint", "precision": "DOUBLE"},
        "nullable": true, "children": [],
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 64}
          ]
        }
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "foo",
          "count": 5,
          "DATA": [1, 2, 3, 4, 5],
          "VALIDITY": [1, 0, 1, 1, 1]
        },
        {
          "name": "bar",
          "count": 5,
          "DATA": [1.0, 2.0, 3.0, 4.0, 5.0],
          "VALIDITY": [1, 0, 0, 1, 1]
        }
      ]
    },
    {
      "count": 4,
      "columns": [
        {
          "name": "foo",
          "count": 4,
          "DATA": [1, 2, 3, 4],
          "VALIDITY": [1, 0, 1, 1]
        },
        {
          "name": "bar",
          "count": 4,
          "DATA": [1.0, 2.0, 3.0, 4.0],
          "VALIDITY": [1, 0, 0, 1]
        }
      ]
    }
  ]
}`

	example3 = `{
  "schema": {
    "fields": [
      {
        "name": "list_nullable",
        "type": {
          "name": "list"
        },
        "nullable": true,
        "children": [
          {
            "name": "item",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          }
        ]
      },
      {
        "name": "struct_nullable",
        "type": {
          "name": "struct"
        },
        "nullable": true,
        "children": [
          {
            "name": "f1",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          },
          {
            "name": "f2",
            "type": {
              "name": "utf8"
            },
            "nullable": true,
            "children": []
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 7,
      "columns": [
        {
          "name": "list_nullable",
          "count": 7,
          "VALIDITY": [
            1,
            1,
            0,
            1,
            0,
            0,
            0
          ],
          "OFFSET": [
            0,
            4,
            8,
            8,
            12,
            12,
            12,
            12
          ],
          "children": [
            {
              "name": "item",
              "count": 12,
              "VALIDITY": [
                0,
                1,
                1,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                -1801578480,
                -340485928,
                -259730121,
                -1430069283,
                900706715,
                -630883814,
                -1732791456,
                237606,
                -1410464938,
                -854246234,
                -818241763,
                893338527
              ]
            }
          ]
        },
        {
          "name": "struct_nullable",
          "count": 7,
          "VALIDITY": [
            0,
            0,
            1,
            1,
            1,
            1,
            0
          ],
          "children": [
            {
              "name": "f1",
              "count": 7,
              "VALIDITY": [
                0,
                0,
                0,
                1,
                0,
                0,
                1
              ],
              "DATA": [
                -946762941,
                970770270,
                836792346,
                -724187464,
                1289964601,
                -310139442,
                -1169064100
              ]
            },
            {
              "name": "f2",
              "count": 7,
              "VALIDITY": [
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "OFFSET": [
                0,
                7,
                14,
                21,
                21,
                21,
                28,
                35
              ],
              "DATA": [
                "85r6rN5",
                "jrDJR09",
                "YfZLF07",
                "",
                "",
                "8jDK2tj",
                "146gCTu"
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 10,
      "columns": [
        {
          "name": "list_nullable",
          "count": 10,
          "VALIDITY": [
            0,
            1,
            1,
            0,
            1,
            1,
            1,
            0,
            0,
            1
          ],
          "OFFSET": [
            0,
            0,
            4,
            8,
            8,
            9,
            11,
            11,
            11,
            11,
            11
          ],
          "children": [
            {
              "name": "item",
              "count": 11,
              "VALIDITY": [
                0,
                0,
                1,
                0,
                0,
                1,
                0,
                1,
                1,
                0,
                0
              ],
              "DATA": [
                103442105,
                2117865335,
                2968015,
                -939072509,
                1200505245,
                1556114914,
                -1326977325,
                139341080,
                -1938030331,
                1500012688,
                955080825
              ]
            }
          ]
        },
        {
          "name": "struct_nullable",
          "count": 10,
          "VALIDITY": [
            0,
            0,
            0,
            1,
            0,
            1,
            0,
            1,
            0,
            1
          ],
          "children": [
            {
              "name": "f1",
              "count": 10,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                0,
                1
              ],
              "DATA": [
                1689993413,
                -379972520,
                441054176,
                -906001202,
                -968548682,
                -450386762,
                916507802,
                -1408175154,
                152570373,
                2072407432
              ]
            },
            {
              "name": "f2",
              "count": 10,
              "VALIDITY": [
                0,
                0,
                0,
                1,
                1,
                1,
                1,
                0,
                0,
                1
              ],
              "OFFSET": [
                0,
                0,
                0,
                0,
                7,
                14,
                21,
                28,
                28,
                28,
                35
              ],
              "DATA": [
                "",
                "",
                "",
                "DhKdnsR",
                "20Zs1ez",
                "ky5eL4K",
                "UkTkWDX",
                "",
                "",
                "OR0xSBn"
              ]
            }
          ]
        }
      ]
    }
  ]
}`
)
