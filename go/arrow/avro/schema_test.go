package avro

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
)

func TestSchemaStringEqual(t *testing.T) {
	tests := []struct {
		avroSchema  string
		arrowSchema []arrow.Field
	}{
		{
			avroSchema: `{
	    "type": "record",
	    "name": "Example",
	    "doc": "A simple name (attribute) and no namespace attribute: use the null namespace (\"\"); the fullname is 'Example'.",
	    "fields": [{
	            "name": "inheritNull",
	            "type": {
	                "type": "enum",
	                "name": "Simple",
	                "doc": "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.",
	                "symbols": ["a", "b"]
	            }
	        }, {
	            "name": "explicitNamespace",
	            "type": {
	                "type": "fixed",
	                "name": "Simple",
	                "namespace": "explicit",
	                "doc": "A simple name (attribute) and a namespace (attribute); the fullname is 'explicit.Simple' (this is a different type than of the 'inheritNull' field).",
	                "size": 12
	            }
	        }, {
	            "name": "fullName",
	            "type": {
	                "type": "record",
	                "name": "a.full.Name",
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
	                        "type": "fixed",
	                        "size": 16,
	                        "name": "md5"
	                    }
	                ]
	            }
	        }, {
	            "name": "id",
	            "type": "int"
	        }, {
	            "name": "bigId",
	            "type": "long"
	        }, {
	            "name": "temperature",
	            "type": [
	                "null",
	                "float"
	            ]
	        }, {
	            "name": "fraction",
	            "type": [
	                "null",
	                "double"
	            ]
	        }, {
	            "name": "is_emergency",
	            "type": "boolean"
	        }, {
	            "name": "remote_ip",
	            "type": [
	                "null",
	                "bytes"
	            ]
	        }, {
	            "name": "person",
	            "type": "record",
	            "fields": [{
	                    "name": "lastname",
	                    "type": "string"
	                }, {
	                    "name": "address",
	                    "type": {
	                        "type": "record",
	                        "name": "AddressUSRecord",
	                        "fields": [{
	                                "name": "streetaddress",
	                                "type": "string"
	                            }, {
	                                "name": "city",
	                                "type": "string"
	                            }
	                        ]
	                    }
	                }, {
	                    "name": "mapfield",
	                    "type": {
	                        "type": "map",
	                        "values": "long",
	                        "default": {}
	                    }
	                }, {
	                    "name": "arrayField",
	                    "type": {
	                        "type": "array",
	                        "items": "string",
	                        "default": []
	                    }
	                }
	            ]
	        }, {
	            "name": "decimalField",
	            "type": {
	                "type": "bytes",
	                "logicalType": "decimal",
	                "precision": 4,
	                "scale": 2
	            }
	        }, {
	            "name": "uuidField",
	            "type": "string",
	            "logicalType": "uuid"
	        }, {
	            "name": "time-millis",
	            "type": "int",
	            "logicalType": "time-millis"
	        }, {
	            "name": "time-micros",
	            "type": "long",
	            "logicalType": "time-micros"
	        }, {
	            "name": "timestamp-millis",
	            "type": "long",
	            "logicalType": "timestamp-millis"
	        }, {
	            "name": "timestamp-micros",
	            "type": "long",
	            "logicalType": "timestamp-micros"
	        }, {
	            "name": "local-timestamp-millis",
	            "type": "long",
	            "logicalType": "local-timestamp-millis"
	        }, {
	            "type": "fixed",
	            "size": 12,
				"logicalType": "duration",
	            "name": "duration"
	        }, {
				"name": "date",
				"type": {
						  "type": "int",
						  "logicalType": "date"
						}
			}
	    ]
	}

`,
			arrowSchema: []arrow.Field{
				{Name: "inheritNull",
					Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{"0": "a", "1": "b"})},
				{Name: "explicitNamespace",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 12},
				},
				{Name: "fullName",
					Type: arrow.StructOf(
						arrow.Field{Name: "inheritNamespace",
							Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false}},
						arrow.Field{Name: "md5",
							Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
					),
				},
				{Name: "id",
					Type: arrow.PrimitiveTypes.Int32,
				},
				{Name: "bigId",
					Type: arrow.PrimitiveTypes.Int64,
				},
				{Name: "temperature",
					Type: arrow.PrimitiveTypes.Float32,
				},
				{Name: "fraction",
					Type: arrow.PrimitiveTypes.Float64,
				},
				{Name: "is_emergency",
					Type: arrow.FixedWidthTypes.Boolean,
				},
				{Name: "remote_ip",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 8},
				},
				{Name: "person",
					Type: arrow.StructOf(
						arrow.Field{Name: "lastname",
							Type: arrow.BinaryTypes.String},
						arrow.Field{Name: "address",
							Type: arrow.StructOf(
								arrow.Field{Name: "streetaddress",
									Type: arrow.BinaryTypes.String},
								arrow.Field{Name: "city",
									Type: arrow.BinaryTypes.String},
							),
						},
						arrow.Field{Name: "mapfield",
							Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
						},
						arrow.Field{Name: "arrayField",
							Type: arrow.ListOf(arrow.BinaryTypes.String),
						},
					),
				},
				{Name: "decimalField",
					Type: &arrow.Decimal128Type{Precision: 4, Scale: 2},
				},
				{Name: "uuidField",
					Type: arrow.BinaryTypes.String,
				},
				{Name: "time-millis",
					Type: arrow.FixedWidthTypes.Time32ms,
				},
				{Name: "time-micros",
					Type: arrow.FixedWidthTypes.Time64us,
				},
				{Name: "timestamp-millis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{Name: "timestamp-micros",
					Type: arrow.FixedWidthTypes.Timestamp_us,
				},
				{Name: "local-timestamp-millis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{Name: "duration",
					Type: arrow.FixedWidthTypes.MonthDayNanoInterval,
				},
				{Name: "date",
					Type: arrow.FixedWidthTypes.Date32,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := arrow.NewSchema(test.arrowSchema, nil)
			got, err := ArrowSchemaFromAvro([]byte(test.avroSchema), false)
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

func TestSchemaEqual(t *testing.T) {
	tests := []struct {
		avroSchema  string
		arrowSchema []arrow.Field
	}{
		{
			avroSchema: `{
	    "type": "record",
	    "name": "Example",
	    "doc": "A simple name (attribute) and no namespace attribute: use the null namespace (\"\"); the fullname is 'Example'.",
	    "fields": [{
	            "name": "inheritNull",
	            "type": {
	                "type": "enum",
	                "name": "Simple",
	                "doc": "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.",
	                "symbols": ["a", "b"]
	            }
	        }, {
	            "name": "explicitNamespace",
	            "type": {
	                "type": "fixed",
	                "name": "Simple",
	                "namespace": "explicit",
	                "doc": "A simple name (attribute) and a namespace (attribute); the fullname is 'explicit.Simple' (this is a different type than of the 'inheritNull' field).",
	                "size": 12
	            }
	        }, {
	            "name": "fullName",
	            "type": {
	                "type": "record",
	                "name": "a.full.Name",
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
	                        "type": "fixed",
	                        "size": 16,
	                        "name": "md5"
	                    }
	                ]
	            }
	        }, {
	            "name": "id",
	            "type": "int"
	        }, {
	            "name": "bigId",
	            "type": "long"
	        }, {
	            "name": "temperature",
	            "type": [
	                "null",
	                "float"
	            ]
	        }, {
	            "name": "fraction",
	            "type": [
	                "null",
	                "double"
	            ]
	        }, {
	            "name": "is_emergency",
	            "type": "boolean"
	        }, {
	            "name": "remote_ip",
	            "type": [
	                "null",
	                "bytes"
	            ]
	        }, {
	            "name": "person",
	            "type": "record",
	            "fields": [{
	                    "name": "lastname",
	                    "type": "string"
	                }, {
	                    "name": "address",
	                    "type": {
	                        "type": "record",
	                        "name": "AddressUSRecord",
	                        "fields": [{
	                                "name": "streetaddress",
	                                "type": "string"
	                            }, {
	                                "name": "city",
	                                "type": "string"
	                            }
	                        ]
	                    }
	                }, {
	                    "name": "mapfield",
	                    "type": {
	                        "type": "map",
	                        "values": "long",
	                        "default": {}
	                    }
	                }, {
	                    "name": "arrayField",
	                    "type": {
	                        "type": "array",
	                        "items": "string",
	                        "default": []
	                    }
	                }
	            ]
	        }, {
	            "name": "decimalField",
	            "type": {
	                "type": "bytes",
	                "logicalType": "decimal",
	                "precision": 4,
	                "scale": 2
	            }
	        }, {
	            "name": "uuidField",
	            "type": "string",
	            "logicalType": "uuid"
	        }, {
	            "name": "time-millis",
	            "type": "int",
	            "logicalType": "time-millis"
	        }, {
	            "name": "time-micros",
	            "type": "long",
	            "logicalType": "time-micros"
	        }, {
	            "name": "timestamp-millis",
	            "type": "long",
	            "logicalType": "timestamp-millis"
	        }, {
	            "name": "timestamp-micros",
	            "type": "long",
	            "logicalType": "timestamp-micros"
	        }, {
	            "name": "local-timestamp-millis",
	            "type": "long",
	            "logicalType": "local-timestamp-millis"
	        }, {
	            "type": "fixed",
	            "size": 12,
				"logicalType": "duration",
	            "name": "duration"
	        }, {
				"name": "date",
				"type": {
						  "type": "int",
						  "logicalType": "date"
						}
			}
	    ]
	}

`,
			arrowSchema: []arrow.Field{
				{Name: "inheritNull",
					Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{"0": "a", "1": "b"})},
				{Name: "explicitNamespace",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 12},
				},
				{Name: "fullName",
					Type: arrow.StructOf(
						arrow.Field{Name: "inheritNamespace",
							Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false}},
						arrow.Field{Name: "md5",
							Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
					),
				},
				{Name: "id",
					Type: arrow.PrimitiveTypes.Int32,
				},
				{Name: "bigId",
					Type: arrow.PrimitiveTypes.Int64,
				},
				{Name: "temperature",
					Type: arrow.PrimitiveTypes.Float32,
				},
				{Name: "fraction",
					Type: arrow.PrimitiveTypes.Float64,
				},
				{Name: "is_emergency",
					Type: arrow.FixedWidthTypes.Boolean,
				},
				{Name: "remote_ip",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 8},
				},
				{Name: "person",
					Type: arrow.StructOf(
						arrow.Field{Name: "lastname",
							Type: arrow.BinaryTypes.String},
						arrow.Field{Name: "address",
							Type: arrow.StructOf(
								arrow.Field{Name: "streetaddress",
									Type: arrow.BinaryTypes.String},
								arrow.Field{Name: "city",
									Type: arrow.BinaryTypes.String},
							),
						},
						arrow.Field{Name: "mapfield",
							Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
						},
						arrow.Field{Name: "arrayField",
							Type: arrow.ListOf(arrow.BinaryTypes.String),
						},
					),
				},
				{Name: "decimalField",
					Type: &arrow.Decimal128Type{Precision: 4, Scale: 2},
				},
				{Name: "uuidField",
					Type: arrow.BinaryTypes.String,
				},
				{Name: "time-millis",
					Type: arrow.FixedWidthTypes.Time32ms,
				},
				{Name: "time-micros",
					Type: arrow.FixedWidthTypes.Time64us,
				},
				{Name: "timestamp-millis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{Name: "timestamp-micros",
					Type: arrow.FixedWidthTypes.Timestamp_us,
				},
				{Name: "local-timestamp-millis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{Name: "duration",
					Type: arrow.FixedWidthTypes.MonthDayNanoInterval,
				},
				{Name: "date",
					Type: arrow.FixedWidthTypes.Date32,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := arrow.NewSchema(test.arrowSchema, nil)
			got, err := ArrowSchemaFromAvro([]byte(test.avroSchema), false)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !got.Equal(want) {
				t.Fatalf("got=\n%v,\n want=\n%v", got.String(), want.String())
			} else {
				t.Logf("got.Equal(want)")
			}
		})
	}
}
