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

package arrjson

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestReadWrite(t *testing.T) {
	wantJSONs := make(map[string]string)
	wantJSONs["nulls"] = makeNullWantJSONs()
	wantJSONs["primitives"] = makePrimitiveWantJSONs()
	wantJSONs["structs"] = makeStructsWantJSONs()
	wantJSONs["lists"] = makeListsWantJSONs()
	wantJSONs["list_views"] = makeListViewsWantJSONs()
	wantJSONs["strings"] = makeStringsWantJSONs()
	wantJSONs["fixed_size_lists"] = makeFixedSizeListsWantJSONs()
	wantJSONs["fixed_width_types"] = makeFixedWidthTypesWantJSONs()
	wantJSONs["fixed_size_binaries"] = makeFixedSizeBinariesWantJSONs()
	wantJSONs["intervals"] = makeIntervalsWantJSONs()
	wantJSONs["durations"] = makeDurationsWantJSONs()
	wantJSONs["decimal128"] = makeDecimal128sWantJSONs()
	wantJSONs["decimal256"] = makeDecimal256sWantJSONs()
	wantJSONs["maps"] = makeMapsWantJSONs()
	wantJSONs["extension"] = makeExtensionsWantJSONs()
	wantJSONs["dictionary"] = makeDictionaryWantJSONs()
	wantJSONs["union"] = makeUnionWantJSONs()
	wantJSONs["run_end_encoded"] = makeRunEndEncodedWantJSONs()
	tempDir := t.TempDir()

	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			f, err := os.CreateTemp(tempDir, "go-arrow-read-write-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			w, err := NewWriter(f, recs[0].Schema())
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()

			for i, rec := range recs {
				err = w.Write(rec)
				if err != nil {
					t.Fatalf("could not write record[%d] to JSON: %v", i, err)
				}
			}

			err = w.Close()
			if err != nil {
				t.Fatalf("could not close JSON writer: %v", err)
			}

			err = f.Sync()
			if err != nil {
				t.Fatalf("could not sync data to disk: %v", err)
			}

			fileBytes, _ := os.ReadFile(f.Name())
			assert.JSONEq(t, wantJSONs[name], string(fileBytes))

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				t.Fatalf("could not rewind file: %v", err)
			}

			r, err := NewReader(f, WithAllocator(mem), WithSchema(recs[0].Schema()))
			if err != nil {
				raw, _ := os.ReadFile(f.Name())
				t.Fatalf("could not read JSON file: %v\n%v\n", err, string(raw))
			}
			defer r.Release()

			r.Retain()
			r.Release()

			if got, want := r.Schema(), recs[0].Schema(); !got.Equal(want) {
				t.Fatalf("invalid schema\ngot:\n%v\nwant:\n%v\n", got, want)
			}

			if got, want := r.NumRecords(), len(recs); got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}

			nrecs := 0
			for {
				rec, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("could not read record[%d]: %v", nrecs, err)
				}

				if !array.RecordEqual(rec, recs[nrecs]) {
					t.Fatalf("records[%d] differ", nrecs)
				}
				nrecs++
			}

			if got, want := nrecs, len(recs); got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}
		})
	}
}

func makeNullWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "nulls",
        "type": {
          "name": "null"
        },
        "nullable": true,
        "children": []
      }
    ],
    "metadata": [
      {
        "key": "k1",
        "value": "v1"
      },
      {
        "key": "k2",
        "value": "v2"
      },
      {
        "key": "k3",
        "value": "v3"
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    }
  ]
}`
}

func makePrimitiveWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "bools",
        "type": {
          "name": "bool"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int8s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 8
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int16s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 16
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int32s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int64s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 64
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint8s",
        "type": {
          "name": "int",
          "bitWidth": 8
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint16s",
        "type": {
          "name": "int",
          "bitWidth": 16
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint32s",
        "type": {
          "name": "int",
          "bitWidth": 32
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint64s",
        "type": {
          "name": "int",
          "bitWidth": 64
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "float32s",
        "type": {
          "name": "floatingpoint",
          "precision": "SINGLE"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "float64s",
        "type": {
          "name": "floatingpoint",
          "precision": "DOUBLE"
        },
        "nullable": true,
        "children": []
      }
    ],
    "metadata": [
      {
        "key": "k1",
        "value": "v1"
      },
      {
        "key": "k2",
        "value": "v2"
      },
      {
        "key": "k3",
        "value": "v3"
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-1",
            "0",
            "0",
            "-4",
            "-5"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-11",
            "0",
            "0",
            "-14",
            "-15"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-21",
            "0",
            "0",
            "-24",
            "-25"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        }
      ]
    }
  ]
}`
}

func makeStructsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
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
            "nullable": false,
            "children": []
          },
          {
            "name": "f2",
            "type": {
              "name": "utf8"
            },
            "nullable": false,
            "children": []
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 25,
      "columns": [
        {
          "name": "struct_nullable",
          "count": 25,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "f1",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25,
                -31,
                0,
                0,
                -34,
                -35,
                -41,
                0,
                0,
                -44,
                -45
              ]
            },
            {
              "name": "f2",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                "111",
                "",
                "",
                "444",
                "555",
                "1111",
                "",
                "",
                "1444",
                "1555",
                "2111",
                "",
                "",
                "2444",
                "2555",
                "3111",
                "",
                "",
                "3444",
                "3555",
                "4111",
                "",
                "",
                "4444",
                "4555"
              ],
              "OFFSET": [
                0,
                3,
                3,
                3,
                6,
                9,
                13,
                13,
                13,
                17,
                21,
                25,
                25,
                25,
                29,
                33,
                37,
                37,
                37,
                41,
                45,
                49,
                49,
                49,
                53,
                57
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 25,
      "columns": [
        {
          "name": "struct_nullable",
          "count": 25,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1
          ],
          "children": [
            {
              "name": "f1",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                1,
                0,
                0,
                4,
                5,
                11,
                0,
                0,
                14,
                15,
                21,
                0,
                0,
                24,
                25,
                31,
                0,
                0,
                34,
                35,
                41,
                0,
                0,
                44,
                45
              ]
            },
            {
              "name": "f2",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                "-111",
                "",
                "",
                "-444",
                "-555",
                "-1111",
                "",
                "",
                "-1444",
                "-1555",
                "-2111",
                "",
                "",
                "-2444",
                "-2555",
                "-3111",
                "",
                "",
                "-3444",
                "-3555",
                "-4111",
                "",
                "",
                "-4444",
                "-4555"
              ],
              "OFFSET": [
                0,
                4,
                4,
                4,
                8,
                12,
                17,
                17,
                17,
                22,
                27,
                32,
                32,
                32,
                37,
                42,
                47,
                47,
                47,
                52,
                57,
                62,
                62,
                62,
                67,
                72
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeListsWantJSONs() string {
	return `{
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
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                1,
                0,
                0,
                4,
                5,
                11,
                0,
                0,
                14,
                15,
                21,
                0,
                0,
                24,
                25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            0,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ]
        }
      ]
    },
    {
      "count": 0,
      "columns": [
        {
          "name": "list_nullable",
          "count": 0,
          "children": [
            {
              "name": "item",
              "count": 0
            }
          ],
          "OFFSET": [
            0
          ]
        }
      ]
    }
  ]
}`
}

func makeListViewsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "list_view_nullable",
        "type": {
          "name": "listview"
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
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "list_view_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                1,
                0,
                0,
                4,
                5,
                11,
                0,
                0,
                14,
                15,
                21,
                0,
                0,
                24,
                25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            10
          ],
          "SIZE": [
            5,
            5,
            5
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_view_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            10
          ],
          "SIZE": [
            5,
            5,
            5
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_view_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            0,
            1
          ],
          "children": [
            {
              "name": "item",
              "count": 10,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ],
          "OFFSET": [
            0,
            5,
            5
          ],
          "SIZE": [
            5,
            0,
            5
          ]
        }
      ]
    },
    {
      "count": 0,
      "columns": [
        {
          "name": "list_view_nullable",
          "count": 0,
          "children": [
            {
              "name": "item",
              "count": 0
            }
          ],
          "OFFSET": [
          ],
          "SIZE": [
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedSizeListsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "fixed_size_list_nullable",
        "type": {
          "name": "fixedsizelist",
          "listSize": 3
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
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                1,
                0,
                3,
                11,
                0,
                13,
                21,
                0,
                23
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                -1,
                0,
                -3,
                -11,
                0,
                -13,
                -21,
                0,
                -23
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            0,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                -1,
                0,
                -3,
                -11,
                0,
                -13,
                -21,
                0,
                -23
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeStringsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "strings",
        "type": {
          "name": "utf8"
        },
        "nullable": false,
        "children": []
      },
      {
        "name": "bytes",
        "type": {
          "name": "binary"
        },
        "nullable": false,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1é",
            "2",
            "3",
            "4",
            "5"
          ],
          "OFFSET": [
            0,
            3,
            4,
            5,
            6,
            7
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "31C3A9",
            "32",
            "33",
            "34",
            "35"
          ],
          "OFFSET": [
            0,
            3,
            4,
            5,
            6,
            7
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "22",
            "33",
            "44",
            "55"
          ],
          "OFFSET": [
            0,
            2,
            4,
            6,
            8,
            10
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "3131",
            "3232",
            "3333",
            "3434",
            "3535"
          ],
          "OFFSET": [
            0,
            2,
            4,
            6,
            8,
            10
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "111",
            "222",
            "333",
            "444",
            "555"
          ],
          "OFFSET": [
            0,
            3,
            6,
            9,
            12,
            15
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "313131",
            "323232",
            "333333",
            "343434",
            "353535"
          ],
          "OFFSET": [
            0,
            3,
            6,
            9,
            12,
            15
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedWidthTypesWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "float16s",
        "type": {
          "name": "floatingpoint",
          "precision": "HALF"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time32ms",
        "type": {
          "name": "time",
          "bitWidth": 32,
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time32s",
        "type": {
          "name": "time",
          "bitWidth": 32,
          "unit": "SECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time64ns",
        "type": {
          "name": "time",
          "bitWidth": 64,
          "unit": "NANOSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time64us",
        "type": {
          "name": "time",
          "bitWidth": 64,
          "unit": "MICROSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_s",
        "type": {
          "name": "timestamp",
          "unit": "SECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_ms",
        "type": {
          "name": "timestamp",
          "unit": "MILLISECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_us",
        "type": {
          "name": "timestamp",
          "unit": "MICROSECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_ns",
        "type": {
          "name": "timestamp",
          "unit": "NANOSECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "date32s",
        "type": {
          "name": "date",
          "unit": "DAY"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "date64s",
        "type": {
          "name": "date",
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedSizeBinariesWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "fixed_size_binary_3",
        "type": {
          "name": "fixedsizebinary",
          "byteWidth": 3
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303031",
            "303032",
            "303033",
            "303034",
            "303035"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303131",
            "303132",
            "303133",
            "303134",
            "303135"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303231",
            "303232",
            "303233",
            "303234",
            "303235"
          ]
        }
      ]
    }
  ]
}`
}

func makeIntervalsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "months",
        "type": {
          "name": "interval",
          "unit": "YEAR_MONTH"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "days",
        "type": {
          "name": "interval",
          "unit": "DAY_TIME"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "nanos",
        "type": {
          "name": "interval",
          "unit": "MONTH_DAY_NANO"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "months",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "days",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "days": 1,
              "milliseconds": 1
            },
            {
              "days": 2,
              "milliseconds": 2
            },
            {
              "days": 3,
              "milliseconds": 3
            },
            {
              "days": 4,
              "milliseconds": 4
            },
            {
              "days": 5,
              "milliseconds": 5
            }
          ]
        },
        {
          "name": "nanos",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "months": 1,
              "days": 1,
              "nanoseconds": 1000
            },
            {
              "months": 2,
              "days": 2,
              "nanoseconds": 2000
            },
            {
              "months": 3,
              "days": 3,
              "nanoseconds": 3000
            },
            {
              "months": 4,
              "days": 4,
              "nanoseconds": 4000
            },
            {
              "months": 5,
              "days": 5,
              "nanoseconds": 5000
            }
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "months",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "days",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "days": -11,
              "milliseconds": -11
            },
            {
              "days": -12,
              "milliseconds": -12
            },
            {
              "days": -13,
              "milliseconds": -13
            },
            {
              "days": -14,
              "milliseconds": -14
            },
            {
              "days": -15,
              "milliseconds": -15
            }
          ]
        },
        {
          "name": "nanos",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "months": -11,
              "days": -11,
              "nanoseconds": -11000
            },
            {
              "months": -12,
              "days": -12,
              "nanoseconds": -12000
            },
            {
              "months": -13,
              "days": -13,
              "nanoseconds": -13000
            },
            {
              "months": -14,
              "days": -14,
              "nanoseconds": -14000
            },
            {
              "months": -15,
              "days": -15,
              "nanoseconds": -15000
            }
          ]
        }
      ]
    },
    {
      "count": 6,
      "columns": [
        {
          "name": "months",
          "count": 6,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25,
            0
          ]
        },
        {
          "name": "days",
          "count": 6,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1,
            1
          ],
          "DATA": [
            {
              "days": 21,
              "milliseconds": 21
            },
            {
              "days": 22,
              "milliseconds": 22
            },
            {
              "days": 23,
              "milliseconds": 23
            },
            {
              "days": 24,
              "milliseconds": 24
            },
            {
              "days": 25,
              "milliseconds": 25
            },
            {
              "days": 0,
              "milliseconds": 0
            }
          ]
        },
        {
          "name": "nanos",
          "count": 6,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1,
            1
          ],
          "DATA": [
            {
              "months": 21,
              "days": 21,
              "nanoseconds": 21000
            },
            {
              "months": 22,
              "days": 22,
              "nanoseconds": 22000
            },
            {
              "months": 23,
              "days": 23,
              "nanoseconds": 23000
            },
            {
              "months": 24,
              "days": 24,
              "nanoseconds": 24000
            },
            {
              "months": 25,
              "days": 25,
              "nanoseconds": 25000
            },
            {
              "months": 0,
              "days": 0,
              "nanoseconds": 0
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeDurationsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "durations-s",
        "type": {
          "name": "duration",
          "unit": "SECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-ms",
        "type": {
          "name": "duration",
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-us",
        "type": {
          "name": "duration",
          "unit": "MICROSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-ns",
        "type": {
          "name": "duration",
          "unit": "NANOSECOND"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        }
      ]
    }
  ]
}`
}

func makeDecimal128sWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "dec128s",
        "type": {
          "name": "decimal",
          "scale": 1,
          "precision": 10,
          "bitWidth": 128
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "571849066284996100127",
            "590295810358705651744",
            "608742554432415203361",
            "627189298506124754978",
            "645636042579834306595"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "756316507022091616297",
            "774763251095801167914",
            "793209995169510719531",
            "811656739243220271148",
            "830103483316929822765"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "940783947759187132467",
            "959230691832896684084",
            "977677435906606235701",
            "996124179980315787318",
            "1014570924054025338935"
          ]
        }
      ]
    }
  ]
}`
}

func makeDecimal256sWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "dec256s",
        "type": {
          "name": "decimal",
          "scale": 2,
          "precision": 72,
          "bitWidth": 256
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "dec256s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "131819136443120296047697507592700702471267712715359757795349",
            "138096238178506976811873579382829307350851889511329270071318",
            "144373339913893657576049651172957912230436066307298782347287",
            "150650441649280338340225722963086517110020243103268294623256",
            "156927543384667019104401794753215121989604419899237806899225"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec256s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "194590153796987103689458225493986751267109480675054880555039",
            "200867255532373784453634297284115356146693657471024392831008",
            "207144357267760465217810369074243961026277834266993905106977",
            "213421459003147145981986440864372565905862011062963417382946",
            "219698560738533826746162512654501170785446187858932929658915"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec256s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "257361171150853911331218943395272800062951248634750003314729",
            "263638272886240592095395015185401404942535425430719515590698",
            "269915374621627272859571086975530009822119602226689027866667",
            "276192476357013953623747158765658614701703779022658540142636",
            "282469578092400634387923230555787219581287955818628052418605"
          ]
        }
      ]
    }
  ]
}`
}

func makeMapsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "map_int_utf8",
        "type": {
          "name": "map",
          "keysSorted": true
        },
        "nullable": true,
        "children": [
          {
            "name": "entries",
            "type": {
              "name": "struct"
            },
            "nullable": false,
            "children": [
              {
                "name": "key",
                "type": {
                  "name": "int",
                  "isSigned": true,
                  "bitWidth": 32
                },
                "nullable": false,
                "children": []
              },
              {
                "name": "value",
                "type": {
                  "name": "utf8"
                },
                "nullable": true,
                "children": []
              }
            ]
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 2,
      "columns": [
        {
          "name": "map_int_utf8",
          "count": 2,
          "VALIDITY": [
            1,
            0
          ],
          "children": [
            {
              "name": "entries",
              "count": 50,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "children": [
                {
                  "name": "key",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1
                  ],
                  "DATA": [
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5
                  ]
                },
                {
                  "name": "value",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1
                  ],
                  "DATA": [
                    "111",
                    "",
                    "",
                    "444",
                    "555",
                    "1111",
                    "",
                    "",
                    "1444",
                    "1555",
                    "2111",
                    "",
                    "",
                    "2444",
                    "2555",
                    "3111",
                    "",
                    "",
                    "3444",
                    "3555",
                    "4111",
                    "",
                    "",
                    "4444",
                    "4555",
                    "-111",
                    "",
                    "",
                    "-444",
                    "-555",
                    "-1111",
                    "",
                    "",
                    "-1444",
                    "-1555",
                    "-2111",
                    "",
                    "",
                    "-2444",
                    "-2555",
                    "-3111",
                    "",
                    "",
                    "-3444",
                    "-3555",
                    "-4111",
                    "",
                    "",
                    "-4444",
                    "-4555"
                  ],
                  "OFFSET": [
                    0,
                    3,
                    3,
                    3,
                    6,
                    9,
                    13,
                    13,
                    13,
                    17,
                    21,
                    25,
                    25,
                    25,
                    29,
                    33,
                    37,
                    37,
                    37,
                    41,
                    45,
                    49,
                    49,
                    49,
                    53,
                    57,
                    61,
                    61,
                    61,
                    65,
                    69,
                    74,
                    74,
                    74,
                    79,
                    84,
                    89,
                    89,
                    89,
                    94,
                    99,
                    104,
                    104,
                    104,
                    109,
                    114,
                    119,
                    119,
                    119,
                    124,
                    129
                  ]
                }
              ]
            }
          ],
          "OFFSET": [
            0,
            25,
            50
          ]
        }
      ]
    },
    {
      "count": 2,
      "columns": [
        {
          "name": "map_int_utf8",
          "count": 2,
          "VALIDITY": [
            1,
            0
          ],
          "children": [
            {
              "name": "entries",
              "count": 50,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "children": [
                {
                  "name": "key",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1
                  ],
                  "DATA": [
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5
                  ]
                },
                {
                  "name": "value",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1
                  ],
                  "DATA": [
                    "-111",
                    "",
                    "",
                    "-444",
                    "-555",
                    "-1111",
                    "",
                    "",
                    "-1444",
                    "-1555",
                    "-2111",
                    "",
                    "",
                    "-2444",
                    "-2555",
                    "-3111",
                    "",
                    "",
                    "-3444",
                    "-3555",
                    "-4111",
                    "",
                    "",
                    "-4444",
                    "-4555",
                    "111",
                    "",
                    "",
                    "444",
                    "555",
                    "1111",
                    "",
                    "",
                    "1444",
                    "1555",
                    "2111",
                    "",
                    "",
                    "2444",
                    "2555",
                    "3111",
                    "",
                    "",
                    "3444",
                    "3555",
                    "4111",
                    "",
                    "",
                    "4444",
                    "4555"
                  ],
                  "OFFSET": [
                    0,
                    4,
                    4,
                    4,
                    8,
                    12,
                    17,
                    17,
                    17,
                    22,
                    27,
                    32,
                    32,
                    32,
                    37,
                    42,
                    47,
                    47,
                    47,
                    52,
                    57,
                    62,
                    62,
                    62,
                    67,
                    72,
                    75,
                    75,
                    75,
                    78,
                    81,
                    85,
                    85,
                    85,
                    89,
                    93,
                    97,
                    97,
                    97,
                    101,
                    105,
                    109,
                    109,
                    109,
                    113,
                    117,
                    121,
                    121,
                    121,
                    125,
                    129
                  ]
                }
              ]
            }
          ],
          "OFFSET": [
            0,
            25,
            50
          ]
        }
      ]
    }
  ]
}`
}

func makeDictionaryWantJSONs() string {
	return `{
    "schema": {
      "fields": [
        {
          "name": "dict0",
          "type": {
            "name": "utf8"
          },
          "nullable": true,
          "children": [],
          "dictionary": {
            "id": 0,
            "indexType": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 8
            },
            "isOrdered": false
          }
        },
        {
          "name": "dict1",
          "type": {
            "name": "utf8"
          },
          "nullable": true,
          "children": [],
          "dictionary": {
            "id": 1,
            "indexType": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "isOrdered": false
          }
        },
        {
          "name": "dict2",
          "type": {
            "name": "int",
            "isSigned": true,
            "bitWidth": 64
          },
          "nullable": true,
          "children": [],
          "dictionary": {
            "id": 2,
            "indexType": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 16
            },
            "isOrdered": false
          }
        }
      ]
    },
    "dictionaries": [
      {
        "id": 0,
        "data": {
          "count": 10,
          "columns": [
            {
              "name": "DICT0",
              "count": 10,
              "VALIDITY": [
                1,
                1,
                0,
                0,
                0,
                1,
                1,
                0,
                1,
                0
              ],
              "OFFSET": [
                0,
                7,
                16,
                16,
                16,
                16,
                28,
                39,
                39,
                46,
                46
              ],
              "DATA": [
                "gen3wjf",
                "bbg61\u00b5\u00b0",
                "",
                "",
                "",
                "\u00f4\u00f42n\u20acm\u00a3",
                "jb2b\u20acd\u20ac",
                "",
                "jfjddrg",
                ""
              ]
            }
          ]
        }
      },
      {
        "id": 1,
        "data": {
          "count": 5,
          "columns": [
            {
              "name": "DICT1",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "OFFSET": [
                0,
                8,
                18,
                27,
                35,
                45
              ],
              "DATA": [
                "\u00c2arcall",
                "\u77e23b\u00b0eif",
                "i3ak\u00b0k\u00b5",
                "gp16\u00a3nd",
                "f4\u00b01e\u00c2\u00b0"
              ]
            }
          ]
        }
      },
      {
        "id": 2,
        "data": {
          "count": 50,
          "columns": [
            {
              "name": "DICT2",
              "count": 50,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                0,
                1,
                0,
                0,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
                0,
                0,
                0,
                1,
                0,
                1,
                0,
                1,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                0
              ],
              "DATA": [
                "-2147483648",
                "2147483647",
                "97251241",
                "-315526314",
                "-256834552",
                "-1159355470",
                "800976983",
                "-1728247486",
                "-1784101814",
                "1320684343",
                "-788965748",
                "1298782506",
                "1971840342",
                "686564052",
                "-115364825",
                "1787500433",
                "-123446338",
                "-1973712113",
                "870684092",
                "-994630427",
                "-1826738974",
                "461928552",
                "1374967188",
                "1317234669",
                "1129789963",
                "312195995",
                "1535930156",
                "-1610317326",
                "-721673697",
                "1443186644",
                "-643456149",
                "1132307434",
                "1240578589",
                "379611602",
                "2011416968",
                "165842874",
                "-570054451",
                "893435720",
                "835998817",
                "1223423131",
                "-1677568310",
                "-230900360",
                "-229961726",
                "2113303164",
                "201112068",
                "452691328",
                "-1980985397",
                "675701869",
                "-1802109191",
                "-669843831"
              ]
            }
          ]
        }
      }
    ],
    "batches": [
      {
        "count": 7,
        "columns": [
          {
            "name": "dict0",
            "count": 7,
            "VALIDITY": [
              1,
              1,
              0,
              1,
              0,
              1,
              1
            ],
            "DATA": [
              7,
              6,
              3,
              1,
              2,
              9,
              1
            ]
          },
          {
            "name": "dict1",
            "count": 7,
            "VALIDITY": [
              1,
              1,
              0,
              0,
              0,
              1,
              0
            ],
            "DATA": [
              0,
              0,
              3,
              3,
              4,
              2,
              3
            ]
          },
          {
            "name": "dict2",
            "count": 7,
            "VALIDITY": [
              0,
              1,
              0,
              1,
              1,
              0,
              1
            ],
            "DATA": [
              3,
              11,
              0,
              33,
              5,
              21,
              9
            ]
          }
        ]
      },
      {
        "count": 10,
        "columns": [
          {
            "name": "dict0",
            "count": 10,
            "VALIDITY": [
              0,
              0,
              0,
              1,
              0,
              0,
              1,
              0,
              1,
              1
            ],
            "DATA": [
              9,
              4,
              3,
              9,
              5,
              7,
              9,
              4,
              0,
              9
            ]
          },
          {
            "name": "dict1",
            "count": 10,
            "VALIDITY": [
              0,
              0,
              0,
              1,
              0,
              0,
              1,
              1,
              1,
              0
            ],
            "DATA": [
              1,
              2,
              4,
              3,
              3,
              3,
              2,
              4,
              4,
              4
            ]
          },
          {
            "name": "dict2",
            "count": 10,
            "VALIDITY": [
              0,
              0,
              1,
              1,
              1,
              1,
              0,
              0,
              1,
              0
            ],
            "DATA": [
              24,
              26,
              39,
              4,
              23,
              23,
              6,
              28,
              9,
              49
            ]
          }
        ]
      }
    ]
  }`
}

func makeExtensionsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "p1",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": [],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "parametric-type-1"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": "\u0006\u0000\u0000\u0000"
          }
        ]
      },
      {
        "name": "p2",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": [],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "parametric-type-1"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": "\u000c\u0000\u0000\u0000"
          }
        ]
      },
      {
        "name": "p3",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": [],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "parametric-type-2<param=2>"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": "\u0002\u0000\u0000\u0000"
          }
        ]
      },
      {
        "name": "p4",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": [],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "parametric-type-2<param=3>"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": "\u0003\u0000\u0000\u0000"
          }
        ]
      },
      {
        "name": "p5",
        "type": {
          "name": "struct"
        },
        "nullable": true,
        "children": [
          {
            "name": "a",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 64
            },
            "nullable": false,
            "children": []
          },
          {
            "name": "b",
            "type": {
              "name": "floatingpoint",
              "precision": "DOUBLE"
            },
            "nullable": false,
            "children": []
          }
        ],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "ext-struct-type"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": "ext-struct-type-unique-code"
          }
        ]
      },
      {
        "name": "unreg",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 8
        },
        "nullable": true,
        "children": [],
        "metadata": [
          {
            "key": "k1",
            "value": "v1"
          },
          {
            "key": "k2",
            "value": "v2"
          },
          {
            "key": "ARROW:extension:name",
            "value": "unregistered"
          },
          {
            "key": "ARROW:extension:metadata",
            "value": ""
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "p1",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            1,
            -1,
            2,
            3,
            -1
          ]
        },
        {
          "name": "p2",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            2,
            -1,
            3,
            4,
            -1
          ]
        },
        {
          "name": "p3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            5,
            -1,
            6,
            7,
            8
          ]
        },
        {
          "name": "p4",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            5,
            -1,
            7,
            9,
            -1
          ]
        },
        {
          "name": "p5",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "children": [
            {
              "name": "a",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0
              ],
              "DATA": [
                "1",
                "0",
                "2",
                "3",
                "0"
              ]
            },
            {
              "name": "b",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0
              ],
              "DATA": [
                0.1,
                0,
                0.2,
                0.3,
                0
              ]
            }
          ]
        },
        {
          "name": "unreg",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "p1",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            10,
            -1,
            20,
            30,
            -1
          ]
        },
        {
          "name": "p2",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            20,
            -1,
            30,
            40,
            -1
          ]
        },
        {
          "name": "p3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            50,
            -1,
            60,
            70,
            8
          ]
        },
        {
          "name": "p4",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            50,
            -1,
            70,
            90,
            -1
          ]
        },
        {
          "name": "p5",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "children": [
            {
              "name": "a",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0
              ],
              "DATA": [
                "10",
                "0",
                "20",
                "30",
                "0"
              ]
            },
            {
              "name": "b",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0
              ],
              "DATA": [
                0.01,
                0,
                0.02,
                0.03,
                0
              ]
            }
          ]
        },
        {
          "name": "unreg",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            0
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        }
      ]
    }
  ]
}`
}

func makeUnionWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "sparse",
        "type": {
          "name": "union",
          "mode": "SPARSE",
          "typeIds": [
            5,
            10
          ]
        },
        "nullable": true,
        "children": [
          {
            "name": "u0",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          },
          {
            "name": "u1",
            "type": {
              "name": "int",
              "bitWidth": 8
            },
            "nullable": true,
            "children": []
          }
        ]
      },
      {
        "name": "dense",
        "type": {
          "name": "union",
          "mode": "DENSE",
          "typeIds": [
            5,
            10
          ]
        },
        "nullable": true,
        "children": [
          {
            "name": "u0",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          },
          {
            "name": "u1",
            "type": {
              "name": "int",
              "bitWidth": 8
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
          "name": "sparse",
          "count": 7,
          "VALIDITY": [
            1,
            1,
            1,
            1,
            1,
            1,
            1
          ],
          "TYPE_ID": [
            5,
            10,
            5,
            5,
            10,
            10,
            5
          ],
          "children": [
            {
              "name": "u0",
              "count": 7,
              "VALIDITY": [
                1,
                1,
                1,
                0,
                1,
                1,
                1
              ],
              "DATA": [
                0,
                1,
                2,
                3,
                4,
                5,
                6
              ]
            },
            {
              "name": "u1",
              "count": 7,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                10,
                11,
                12,
                13,
                14,
                15,
                16
              ]
            }
          ]
        },
        {
          "name": "dense",
          "count": 7,
          "VALIDITY": [
            1,
            1,
            1,
            1,
            1,
            1,
            1
          ],
          "TYPE_ID": [
            5,
            10,
            5,
            5,
            10,
            10,
            5
          ],
          "OFFSET": [
            0,
            0,
            1,
            2,
            1,
            2,
            3
          ],
          "children": [
            {
              "name": "u0",
              "count": 4,
              "VALIDITY": [
                1,
                0,
                1,
                1
              ],
              "DATA": [
                0,
                2,
                3,
                7
              ]
            },
            {
              "name": "u1",
              "count": 3,
              "VALIDITY": [
                1,
                1,
                1
              ],
              "DATA": [
                11,
                14,
                15
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 7,
      "columns": [
        {
          "name": "sparse",
          "count": 7,
          "VALIDITY": [
            1,
            1,
            1,
            1,
            1,
            1,
            1
          ],
          "TYPE_ID": [
            5,
            10,
            5,
            5,
            10,
            10,
            5
          ],
          "children": [
            {
              "name": "u0",
              "count": 7,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                0
              ],
              "DATA": [
                0,
                -1,
                -2,
                -3,
                -4,
                -5,
                -6
              ]
            },
            {
              "name": "u1",
              "count": 7,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                100,
                101,
                102,
                103,
                104,
                105,
                106
              ]
            }
          ]
        },
        {
          "name": "dense",
          "count": 7,
          "VALIDITY": [
            1,
            1,
            1,
            1,
            1,
            1,
            1
          ],
          "TYPE_ID": [
            5,
            10,
            5,
            5,
            10,
            10,
            5
          ],
          "OFFSET": [
            0,
            0,
            1,
            2,
            1,
            2,
            3
          ],
          "children": [
            {
              "name": "u0",
              "count": 4,
              "VALIDITY": [
                0,
                1,
                1,
                0
              ],
              "DATA": [
                0,
                -2,
                -3,
                -7
              ]
            },
            {
              "name": "u1",
              "count": 3,
              "VALIDITY": [
                1,
                1,
                1
              ],
              "DATA": [
                101,
                104,
                105
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeRunEndEncodedWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "ree16",
        "type": {
          "name": "runendencoded"
        },
        "nullable": false,
        "children": [
          {
            "name": "run_ends",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 16
            },
            "nullable": false,
            "children": []
          },
          {
            "name": "values",
            "type": {
              "name": "utf8"
            },
            "nullable": true,
            "children": []
          }
        ]
      },
      {
        "name": "ree32",
        "type": {
          "name": "runendencoded"
        },
        "nullable": false,
        "children": [
          {
            "name": "run_ends",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": false,
            "children": []
          },
          {
            "name": "values",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": false,
            "children": []
          }
        ]
      },
      {
        "name": "ree64",
        "type": {
          "name": "runendencoded"
        },
        "nullable": false,
        "children": [
          {
            "name": "run_ends",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 64
            },
            "nullable": false,
            "children": []
          },
          {
            "name": "values",
            "type": {
              "name": "binary"
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
      "count": 1100,
      "columns": [
        {
          "name": "ree16",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 2,
              "VALIDITY": [
                1,
                1
              ],
              "DATA": [
                1000,
                1100
              ]
            },
            {
              "name": "values",
              "count": 2,
              "VALIDITY": [
                0,
                1
              ],
              "DATA": [
                "foo",
                ""
              ],
              "OFFSET": [
                9,
                12,
                12
              ]
            }
          ]
        },
        {
          "name": "ree32",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                100,
                200,
                800,
                1000,
                1100
              ]
            },
            {
              "name": "values",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                -1,
                -2,
                -3,
                -4,
                -5
              ]
            }
          ]
        },
        {
          "name": "ree64",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                "100",
                "250",
                "450",
                "800",
                "1100"
              ]
            },
            {
              "name": "values",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                0,
                1
              ],
              "DATA": [
                "DEAD",
                "BEEF",
                "DEADBEEF",
                "",
                "BAADF00D"
              ],
              "OFFSET": [
                0,
                2,
                4,
                8,
                8,
                12
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 1100,
      "columns": [
        {
          "name": "ree16",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                90,
                140,
                150,
                1050,
                1100
              ]
            },
            {
              "name": "values",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                0,
                1
              ],
              "DATA": [
                "super",
                "dee",
                "",
                "duper",
                "doo"
              ],
              "OFFSET": [
                0,
                5,
                8,
                8,
                13,
                16
              ]
            }
          ]
        },
        {
          "name": "ree32",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                100,
                120,
                710,
                810,
                1100
              ]
            },
            {
              "name": "values",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                -1,
                -2,
                -3,
                -4,
                -5
              ]
            }
          ]
        },
        {
          "name": "ree64",
          "count": 1100,
          "children": [
            {
              "name": "run_ends",
              "count": 5,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1
              ],
              "DATA": [
                "100",
                "250",
                "450",
                "800",
                "1100"
              ]
            },
            {
              "name": "values",
              "count": 5,
              "VALIDITY": [
                1,
                0,
                1,
                0,
                1
              ],
              "DATA": [
                "DEAD",
                "BEEF",
                "DEADBEEF",
                "",
                "BAADF00D"
              ],
              "OFFSET": [
                0,
                2,
                4,
                8,
                8,
                12
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}
