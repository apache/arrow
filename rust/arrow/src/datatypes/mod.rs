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

//! Defines the logical data types of Arrow arrays.
//!
//! The most important things you might be looking for are:
//!  * [`Schema`](crate::datatypes::Schema) to describe a schema.
//!  * [`Field`](crate::datatypes::Field) to describe one field within a schema.
//!  * [`DataType`](crate::datatypes::DataType) to describe the type of a field.

use std::sync::Arc;

mod native;
pub use native::*;
mod field;
pub use field::*;
mod schema;
pub use schema::*;
mod numeric;
pub use numeric::*;
mod types;
pub use types::*;
mod datatype;
pub use datatype::*;

/// A reference-counted reference to a [`Schema`](crate::datatypes::Schema).
pub type SchemaRef = Arc<Schema>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use serde_json::Value::{Bool, Number as VNumber};
    use serde_json::{Number, Value};
    use std::{
        collections::{BTreeMap, HashMap},
        f32::NAN,
    };

    #[test]
    fn test_list_datatype_equality() {
        // tests that list type equality is checked while ignoring list names
        let list_a = DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_b = DataType::List(Box::new(Field::new("array", DataType::Int32, true)));
        let list_c = DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_d = DataType::List(Box::new(Field::new("item", DataType::UInt32, true)));
        assert!(list_a.equals_datatype(&list_b));
        assert!(!list_a.equals_datatype(&list_c));
        assert!(!list_b.equals_datatype(&list_c));
        assert!(!list_a.equals_datatype(&list_d));

        let list_e =
            DataType::FixedSizeList(Box::new(Field::new("item", list_a, false)), 3);
        let list_f =
            DataType::FixedSizeList(Box::new(Field::new("array", list_b, false)), 3);
        let list_g = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::FixedSizeBinary(3), true)),
            3,
        );
        assert!(list_e.equals_datatype(&list_f));
        assert!(!list_e.equals_datatype(&list_g));
        assert!(!list_f.equals_datatype(&list_g));

        let list_h = DataType::Struct(vec![Field::new("f1", list_e, true)]);
        let list_i = DataType::Struct(vec![Field::new("f1", list_f.clone(), true)]);
        let list_j = DataType::Struct(vec![Field::new("f1", list_f.clone(), false)]);
        let list_k = DataType::Struct(vec![
            Field::new("f1", list_f.clone(), false),
            Field::new("f2", list_g.clone(), false),
            Field::new("f3", DataType::Utf8, true),
        ]);
        let list_l = DataType::Struct(vec![
            Field::new("ff1", list_f.clone(), false),
            Field::new("ff2", list_g.clone(), false),
            Field::new("ff3", DataType::LargeUtf8, true),
        ]);
        let list_m = DataType::Struct(vec![
            Field::new("ff1", list_f, false),
            Field::new("ff2", list_g, false),
            Field::new("ff3", DataType::Utf8, true),
        ]);
        assert!(list_h.equals_datatype(&list_i));
        assert!(!list_h.equals_datatype(&list_j));
        assert!(!list_k.equals_datatype(&list_l));
        assert!(list_k.equals_datatype(&list_m));
    }

    #[test]
    fn create_struct_type() {
        let _person = DataType::Struct(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);
    }

    #[test]
    fn serde_struct_type() {
        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: BTreeMap<String, String> = kv_array.iter().cloned().collect();

        // Non-empty map: should be converted as JSON obj { ... }
        let mut first_name = Field::new("first_name", DataType::Utf8, false);
        first_name.set_metadata(Some(field_metadata));

        // Empty map: should be omitted.
        let mut last_name = Field::new("last_name", DataType::Utf8, false);
        last_name.set_metadata(Some(BTreeMap::default()));

        let person = DataType::Struct(vec![
            first_name,
            last_name,
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);

        let serialized = serde_json::to_string(&person).unwrap();

        // NOTE that this is testing the default (derived) serialization format, not the
        // JSON format specified in metadata.md

        assert_eq!(
            "{\"Struct\":[\
             {\"name\":\"first_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{\"k\":\"v\"}},\
             {\"name\":\"last_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},\
             {\"name\":\"address\",\"data_type\":{\"Struct\":\
             [{\"name\":\"street\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},\
             {\"name\":\"zip\",\"data_type\":\"UInt16\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false}\
             ]},\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false}]}",
            serialized
        );

        let deserialized = serde_json::from_str(&serialized).unwrap();

        assert_eq!(person, deserialized);
    }

    #[test]
    fn struct_field_to_json() {
        let f = Field::new(
            "address",
            DataType::Struct(vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ]),
            false,
        );
        let value: Value = serde_json::from_str(
            r#"{
                "name": "address",
                "nullable": false,
                "type": {
                    "name": "struct"
                },
                "children": [
                    {
                        "name": "street",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "zip",
                        "nullable": false,
                        "type": {
                            "name": "int",
                            "bitWidth": 16,
                            "isSigned": false
                        },
                        "children": []
                    }
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(value, f.to_json());
    }

    #[test]
    fn primitive_field_to_json() {
        let f = Field::new("first_name", DataType::Utf8, false);
        let value: Value = serde_json::from_str(
            r#"{
                "name": "first_name",
                "nullable": false,
                "type": {
                    "name": "utf8"
                },
                "children": []
            }"#,
        )
        .unwrap();
        assert_eq!(value, f.to_json());
    }
    #[test]
    fn parse_struct_from_json() {
        let json = r#"
        {
            "name": "address",
            "type": {
                "name": "struct"
            },
            "nullable": false,
            "children": [
                {
                    "name": "street",
                    "type": {
                    "name": "utf8"
                    },
                    "nullable": false,
                    "children": []
                },
                {
                    "name": "zip",
                    "type": {
                    "name": "int",
                    "isSigned": false,
                    "bitWidth": 16
                    },
                    "nullable": false,
                    "children": []
                }
            ]
        }
        "#;
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = Field::from(&value).unwrap();

        let expected = Field::new(
            "address",
            DataType::Struct(vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ]),
            false,
        );

        assert_eq!(expected, dt);
    }

    #[test]
    fn parse_utf8_from_json() {
        let json = "{\"name\":\"utf8\"}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = DataType::from(&value).unwrap();
        assert_eq!(DataType::Utf8, dt);
    }

    #[test]
    fn parse_int32_from_json() {
        let json = "{\"name\": \"int\", \"isSigned\": true, \"bitWidth\": 32}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = DataType::from(&value).unwrap();
        assert_eq!(DataType::Int32, dt);
    }

    #[test]
    fn schema_json() {
        // Add some custom metadata
        let metadata: HashMap<String, String> =
            [("Key".to_string(), "Value".to_string())]
                .iter()
                .cloned()
                .collect();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::Binary, false),
                Field::new("c3", DataType::FixedSizeBinary(3), false),
                Field::new("c4", DataType::Boolean, false),
                Field::new("c5", DataType::Date32, false),
                Field::new("c6", DataType::Date64, false),
                Field::new("c7", DataType::Time32(TimeUnit::Second), false),
                Field::new("c8", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("c9", DataType::Time32(TimeUnit::Microsecond), false),
                Field::new("c10", DataType::Time32(TimeUnit::Nanosecond), false),
                Field::new("c11", DataType::Time64(TimeUnit::Second), false),
                Field::new("c12", DataType::Time64(TimeUnit::Millisecond), false),
                Field::new("c13", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("c14", DataType::Time64(TimeUnit::Nanosecond), false),
                Field::new("c15", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "c16",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
                    false,
                ),
                Field::new(
                    "c17",
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("Africa/Johannesburg".to_string()),
                    ),
                    false,
                ),
                Field::new(
                    "c18",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("c19", DataType::Interval(IntervalUnit::DayTime), false),
                Field::new("c20", DataType::Interval(IntervalUnit::YearMonth), false),
                Field::new(
                    "c21",
                    DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                    false,
                ),
                Field::new(
                    "c22",
                    DataType::FixedSizeList(
                        Box::new(Field::new("bools", DataType::Boolean, false)),
                        5,
                    ),
                    false,
                ),
                Field::new(
                    "c23",
                    DataType::List(Box::new(Field::new(
                        "inner_list",
                        DataType::List(Box::new(Field::new(
                            "struct",
                            DataType::Struct(vec![]),
                            true,
                        ))),
                        false,
                    ))),
                    true,
                ),
                Field::new(
                    "c24",
                    DataType::Struct(vec![
                        Field::new("a", DataType::Utf8, false),
                        Field::new("b", DataType::UInt16, false),
                    ]),
                    false,
                ),
                Field::new("c25", DataType::Interval(IntervalUnit::YearMonth), true),
                Field::new("c26", DataType::Interval(IntervalUnit::DayTime), true),
                Field::new("c27", DataType::Duration(TimeUnit::Second), false),
                Field::new("c28", DataType::Duration(TimeUnit::Millisecond), false),
                Field::new("c29", DataType::Duration(TimeUnit::Microsecond), false),
                Field::new("c30", DataType::Duration(TimeUnit::Nanosecond), false),
                Field::new_dict(
                    "c31",
                    DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Utf8),
                    ),
                    true,
                    123,
                    true,
                ),
                Field::new("c32", DataType::LargeBinary, true),
                Field::new("c33", DataType::LargeUtf8, true),
                Field::new(
                    "c34",
                    DataType::LargeList(Box::new(Field::new(
                        "inner_large_list",
                        DataType::LargeList(Box::new(Field::new(
                            "struct",
                            DataType::Struct(vec![]),
                            false,
                        ))),
                        true,
                    ))),
                    true,
                ),
            ],
            metadata,
        );

        let expected = schema.to_json();
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "type": {
                            "name": "binary"
                        },
                        "children": []
                    },
                    {
                        "name": "c3",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizebinary",
                            "byteWidth": 3
                        },
                        "children": []
                    },
                    {
                        "name": "c4",
                        "nullable": false,
                        "type": {
                            "name": "bool"
                        },
                        "children": []
                    },
                    {
                        "name": "c5",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "DAY"
                        },
                        "children": []
                    },
                    {
                        "name": "c6",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c7",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c8",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c9",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c10",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c11",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c12",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c13",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c14",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c15",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c16",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MILLISECOND",
                            "timezone": "UTC"
                        },
                        "children": []
                    },
                    {
                        "name": "c17",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MICROSECOND",
                            "timezone": "Africa/Johannesburg"
                        },
                        "children": []
                    },
                    {
                        "name": "c18",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c19",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c20",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c21",
                        "nullable": false,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "item",
                                "nullable": true,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c22",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizelist",
                            "listSize": 5
                        },
                        "children": [
                            {
                                "name": "bools",
                                "nullable": false,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c23",
                        "nullable": true,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "inner_list",
                                "nullable": false,
                                "type": {
                                    "name": "list"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": true,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "c24",
                        "nullable": false,
                        "type": {
                            "name": "struct"
                        },
                        "children": [
                            {
                                "name": "a",
                                "nullable": false,
                                "type": {
                                    "name": "utf8"
                                },
                                "children": []
                            },
                            {
                                "name": "b",
                                "nullable": false,
                                "type": {
                                    "name": "int",
                                    "bitWidth": 16,
                                    "isSigned": false
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c25",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c26",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c27",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c28",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c29",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c30",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c31",
                        "nullable": true,
                        "children": [],
                        "type": {
                          "name": "utf8"
                        },
                        "dictionary": {
                          "id": 123,
                          "indexType": {
                            "name": "int",
                            "bitWidth": 32,
                            "isSigned": true
                          },
                          "isOrdered": true
                        }
                    },
                    {
                        "name": "c32",
                        "nullable": true,
                        "type": {
                          "name": "largebinary"
                        },
                        "children": []
                    },
                    {
                        "name": "c33",
                        "nullable": true,
                        "type": {
                          "name": "largeutf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c34",
                        "nullable": true,
                        "type": {
                          "name": "largelist"
                        },
                        "children": [
                            {
                                "name": "inner_large_list",
                                "nullable": true,
                                "type": {
                                    "name": "largelist"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": false,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "metadata" : {
                    "Key": "Value"
                }
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, value);

        // convert back to a schema
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema2 = Schema::from(&value).unwrap();

        assert_eq!(schema, schema2);

        // Check that empty metadata produces empty value in JSON and can be parsed
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ],
                "metadata": {}
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema = Schema::from(&value).unwrap();
        assert!(schema.metadata.is_empty());

        // Check that metadata field is not required in the JSON.
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ]
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema = Schema::from(&value).unwrap();
        assert!(schema.metadata.is_empty());
    }

    #[test]
    fn create_schema_string() {
        let schema = person_schema();
        assert_eq!(schema.to_string(),
        "Field { name: \"first_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: Some({\"k\": \"v\"}) }, \
        Field { name: \"last_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
        Field { name: \"address\", data_type: Struct([\
            Field { name: \"street\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
            Field { name: \"zip\", data_type: UInt16, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }\
        ]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
        Field { name: \"interests\", data_type: Dictionary(Int32, Utf8), nullable: true, dict_id: 123, dict_is_ordered: true, metadata: None }")
    }

    #[test]
    fn schema_field_accessors() {
        let schema = person_schema();

        // test schema accessors
        assert_eq!(schema.fields().len(), 4);

        // test field accessors
        let first_name = &schema.fields()[0];
        assert_eq!(first_name.name(), "first_name");
        assert_eq!(first_name.data_type(), &DataType::Utf8);
        assert_eq!(first_name.is_nullable(), false);
        assert_eq!(first_name.dict_id(), None);
        assert_eq!(first_name.dict_is_ordered(), None);

        let metadata = first_name.metadata();
        assert!(metadata.is_some());
        let md = metadata.as_ref().unwrap();
        assert_eq!(md.len(), 1);
        let key = md.get("k");
        assert!(key.is_some());
        assert_eq!(key.unwrap(), "v");

        let interests = &schema.fields()[3];
        assert_eq!(interests.name(), "interests");
        assert_eq!(
            interests.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(interests.dict_id(), Some(123));
        assert_eq!(interests.dict_is_ordered(), Some(true));
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_index_of() {
        let schema = person_schema();
        assert_eq!(schema.index_of("first_name").unwrap(), 0);
        assert_eq!(schema.index_of("last_name").unwrap(), 1);
        schema.index_of("nickname").unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_field_with_name() {
        let schema = person_schema();
        assert_eq!(
            schema.field_with_name("first_name").unwrap().name(),
            "first_name"
        );
        assert_eq!(
            schema.field_with_name("last_name").unwrap().name(),
            "last_name"
        );
        schema.field_with_name("nickname").unwrap();
    }

    #[test]
    fn schema_field_with_dict_id() {
        let schema = person_schema();

        let fields_dict_123: Vec<_> = schema
            .fields_with_dict_id(123)
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(fields_dict_123, vec!["interests"]);

        assert!(schema.fields_with_dict_id(456).is_empty());
    }

    #[test]
    fn schema_equality() {
        let schema1 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);

        assert_eq!(schema1, schema2);

        let schema3 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float32, true),
        ]);
        let schema4 = Schema::new(vec![
            Field::new("C1", DataType::Utf8, false),
            Field::new("C2", DataType::Float64, true),
        ]);

        assert!(schema1 != schema3);
        assert!(schema1 != schema4);
        assert!(schema2 != schema3);
        assert!(schema2 != schema4);
        assert!(schema3 != schema4);

        let mut f = Field::new("c1", DataType::Utf8, false);
        f.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let schema5 = Schema::new(vec![
            f,
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        assert!(schema1 != schema5);
    }

    #[test]
    fn test_arrow_native_type_to_json() {
        assert_eq!(Some(Bool(true)), true.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i64.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u64.into_json_value());
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01.into_json_value()
        );
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01f64.into_json_value()
        );
        assert_eq!(None, NAN.into_json_value());
    }

    fn person_schema() -> Schema {
        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: BTreeMap<String, String> = kv_array.iter().cloned().collect();
        let mut first_name = Field::new("first_name", DataType::Utf8, false);
        first_name.set_metadata(Some(field_metadata));

        Schema::new(vec![
            first_name,
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
            Field::new_dict(
                "interests",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                123,
                true,
            ),
        ])
    }

    #[test]
    fn test_try_merge_field_with_metadata() {
        // 1. Different values for the same key should cause error.
        let metadata1: BTreeMap<String, String> =
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(metadata1));

        let metadata2: BTreeMap<String, String> =
            [("foo".to_string(), "baz".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(metadata2));

        assert!(
            Schema::try_merge(vec![Schema::new(vec![f1]), Schema::new(vec![f2])])
                .is_err()
        );

        // 2. None + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let metadata2: BTreeMap<String, String> =
            [("missing".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(metadata2));

        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata().is_some());
        assert_eq!(
            f1.metadata().as_ref().unwrap(),
            f2.metadata().as_ref().unwrap()
        );

        // 3. Some + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(
            [("foo2".to_string(), "bar2".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));

        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata().is_some());
        assert_eq!(
            f1.metadata().clone().unwrap(),
            [
                ("foo".to_string(), "bar".to_string()),
                ("foo2".to_string(), "bar2".to_string())
            ]
            .iter()
            .cloned()
            .collect()
        );

        // 4. Some + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata().is_some());
        assert_eq!(
            f1.metadata().clone().unwrap(),
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect()
        );

        // 5. None + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata().is_none());
    }

    #[test]
    fn test_schema_merge() -> Result<()> {
        let merged = Schema::try_merge(vec![
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new(
                    "address",
                    DataType::Struct(vec![Field::new("zip", DataType::UInt16, false)]),
                    false,
                ),
            ]),
            Schema::new_with_metadata(
                vec![
                    // nullable merge
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            // add new nested field
                            Field::new("street", DataType::Utf8, false),
                            // nullable merge on nested field
                            Field::new("zip", DataType::UInt16, true),
                        ]),
                        false,
                    ),
                    // new field
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>(),
            ),
        ])?;

        assert_eq!(
            merged,
            Schema::new_with_metadata(
                vec![
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            Field::new("zip", DataType::UInt16, true),
                            Field::new("street", DataType::Utf8, false),
                        ]),
                        false,
                    ),
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            )
        );

        // support merge union fields
        assert_eq!(
            Schema::try_merge(vec![
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(vec![
                        Field::new("c11", DataType::Utf8, true),
                        Field::new("c12", DataType::Utf8, true),
                    ]),
                    false
                ),]),
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(vec![
                        Field::new("c12", DataType::Utf8, true),
                        Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                    ]),
                    false
                ),])
            ])?,
            Schema::new(vec![Field::new(
                "c1",
                DataType::Union(vec![
                    Field::new("c11", DataType::Utf8, true),
                    Field::new("c12", DataType::Utf8, true),
                    Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                ]),
                false
            ),]),
        );

        // incompatible field should throw error
        assert!(Schema::try_merge(vec![
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
            ]),
            Schema::new(vec![Field::new("last_name", DataType::Int64, false),])
        ])
        .is_err());

        // incompatible metadata should throw error
        assert!(Schema::try_merge(vec![
            Schema::new_with_metadata(
                vec![Field::new("first_name", DataType::Utf8, false)],
                [("foo".to_string(), "bar".to_string()),]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            ),
            Schema::new_with_metadata(
                vec![Field::new("last_name", DataType::Utf8, false)],
                [("foo".to_string(), "baz".to_string()),]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            )
        ])
        .is_err());

        Ok(())
    }
}
