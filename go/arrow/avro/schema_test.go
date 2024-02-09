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

package avro

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	hamba "github.com/hamba/avro/v2"
)

func TestSchemaStringEqual(t *testing.T) {
	tests := []struct {
		avroSchema  string
		arrowSchema []arrow.Field
	}{
		{
			avroSchema: `{
				"fields": [
					{
						"name": "inheritNull",
						"type": {
							"name": "Simple",
							"symbols": [
								"a",
								"b"
							],
							"type": "enum"
						}
					},
					{
						"name": "explicitNamespace",
						"type": {
							"name": "test",
							"namespace": "org.hamba.avro",
							"size": 12,
							"type": "fixed"
						}
					},
					{
						"name": "fullName",
						"type": {
							"type": "record",
							"name": "fullName_data",
							"namespace": "ignored",
							"doc": "A name attribute with a fullname, so the namespace attribute is ignored. The fullname is 'a.full.Name', and the namespace is 'a.full'.",
							"fields": [{
									"name": "inheritNamespace",
									"type": {
										"type": "enum",
										"name": "Understanding",
										"doc": "A simple name (attribute) and no namespace attribute: inherit the namespace of the enclosing type 'a.full.Name'. The fullname is 'a.full.Understanding'.",
										"symbols": ["d", "e"]
									}
								}, {
									"name": "md5",
									"type": {
                                            "name": "md5_data",
                                            "type": "fixed",
									        "size": 16,
									        "namespace": "ignored"
                                    }
								}
							]
						}
					},
					{
						"name": "id",
						"type": "int"
					},
					{
						"name": "bigId",
						"type": "long"
					},
					{
						"name": "temperature",
						"type": [
							"null",
							"float"
						]
					},
					{
						"name": "fraction",
						"type": [
							"null",
							"double"
						]
					},
					{
						"name": "is_emergency",
						"type": "boolean"
					},
					{
						"name": "remote_ip",
						"type": [
							"null",
							"bytes"
						]
					},
					{
						"name": "person",
						"type": {
							"fields": [
								{
									"name": "lastname",
									"type": "string"
								},
								{
									"name": "address",
									"type": {
										"fields": [
											{
												"name": "streetaddress",
												"type": "string"
											},
											{
												"name": "city",
												"type": "string"
											}
										],
										"name": "AddressUSRecord",
										"type": "record"
									}
								},
								{
									"name": "mapfield",
									"type": {
										"default": {
										},
										"type": "map",
										"values": "long"
									}
								},
								{
									"name": "arrayField",
									"type": {
										"default": [
										],
										"items": "string",
										"type": "array"
									}
								}
							],
							"name": "person_data",
							"type": "record"
						}
					},
					{
						"name": "decimalField",
						"type": {
							"logicalType": "decimal",
							"precision": 4,
							"scale": 2,
							"type": "bytes"
						}
					},
					{
						"logicalType": "uuid",
						"name": "uuidField",
						"type": "string"
					},
					{
						"name": "timemillis",
						"type": {
							"type": "int",
							"logicalType": "time-millis"
						}
					},
					{
						"name": "timemicros",
						"type": {
								"type": "long",
								"logicalType": "time-micros"
						}
					},
					{
						"name": "timestampmillis",
						"type": {
							"type": "long",
							"logicalType": "timestamp-millis"
						}
					},
					{
						"name": "timestampmicros",
						"type": {
							"type": "long",
							"logicalType": "timestamp-micros"
						}
					},
					{
						"name": "duration",
						"type": {
							"name": "duration",
							"namespace": "whyowhy",
							"logicalType": "duration",
							"size": 12,
							"type": "fixed"
						}
					},
					{
						"name": "date",
						"type": {
							"logicalType": "date",
							"type": "int"
						}
					}
				],
				"name": "Example",
				"type": "record"
			}`,
			arrowSchema: []arrow.Field{
				{
					Name:     "inheritNull",
					Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
					Metadata: arrow.MetadataFrom(map[string]string{"0": "a", "1": "b"}),
				},
				{
					Name: "explicitNamespace",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 12},
				},
				{
					Name: "fullName",
					Type: arrow.StructOf(
						arrow.Field{
							Name: "inheritNamespace",
							Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
						},
						arrow.Field{
							Name: "md5",
							Type: &arrow.FixedSizeBinaryType{ByteWidth: 16},
						},
					),
				},
				{
					Name: "id",
					Type: arrow.PrimitiveTypes.Int32,
				},
				{
					Name: "bigId",
					Type: arrow.PrimitiveTypes.Int64,
				},
				{
					Name:     "temperature",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
				},
				{
					Name:     "fraction",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
				},
				{
					Name: "is_emergency",
					Type: arrow.FixedWidthTypes.Boolean,
				},
				{
					Name:     "remote_ip",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
				},
				{
					Name: "person",
					Type: arrow.StructOf(
						arrow.Field{
							Name:     "lastname",
							Type:     arrow.BinaryTypes.String,
							Nullable: true,
						},
						arrow.Field{
							Name: "address",
							Type: arrow.StructOf(
								arrow.Field{
									Name: "streetaddress",
									Type: arrow.BinaryTypes.String,
								},
								arrow.Field{
									Name: "city",
									Type: arrow.BinaryTypes.String,
								},
							),
						},
						arrow.Field{
							Name:     "mapfield",
							Type:     arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
							Nullable: true,
						},
						arrow.Field{
							Name: "arrayField",
							Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String),
						},
					),
				},
				{
					Name: "decimalField",
					Type: &arrow.Decimal128Type{Precision: 4, Scale: 2},
				},
				{
					Name: "uuidField",
					Type: arrow.BinaryTypes.String,
				},
				{
					Name: "timemillis",
					Type: arrow.FixedWidthTypes.Time32ms,
				},
				{
					Name: "timemicros",
					Type: arrow.FixedWidthTypes.Time64us,
				},
				{
					Name: "timestampmillis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{
					Name: "timestampmicros",
					Type: arrow.FixedWidthTypes.Timestamp_us,
				},
				{
					Name: "duration",
					Type: arrow.FixedWidthTypes.MonthDayNanoInterval,
				},
				{
					Name: "date",
					Type: arrow.FixedWidthTypes.Date32,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := arrow.NewSchema(test.arrowSchema, nil)
			schema, err := hamba.ParseBytes([]byte(test.avroSchema))
			if err != nil {
				t.Fatalf("%v", err)
			}
			got, err := ArrowSchemaFromAvro(schema)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !(fmt.Sprintf("%+v", want.String()) == fmt.Sprintf("%+v", got.String())) {
				t.Fatalf("got=%v,\n want=%v", got.String(), want.String())
			} else {
				t.Logf("schema.String() comparison passed")
			}
		})
	}
}
