// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]

mod roundtrip_tests {

    use super::super::{super::error::Result, protobuf};
    use crate::error::BallistaError;
    use arrow::datatypes::{DataType, Field, Schema};
    use core::panic;
    use datafusion::physical_plan::functions::BuiltinScalarFunction::Sqrt;
    use datafusion::{
        logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
        physical_plan::csv::CsvReadOptions,
        prelude::*,
        scalar::ScalarValue,
    };
    use protobuf::arrow_type;
    use std::convert::TryInto;

    //Given a identity of a LogicalPlan converts it to protobuf and back, using debug formatting to test equality.
    macro_rules! roundtrip_test {
        ($initial_struct:ident, $proto_type:ty, $struct_type:ty) => {
            let proto: $proto_type = (&$initial_struct).try_into()?;

            let round_trip: $struct_type = (&proto).try_into()?;

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
        ($initial_struct:ident, $struct_type:ty) => {
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, $struct_type);
        };
        ($initial_struct:ident) => {
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, LogicalPlan);
        };
    }

    #[test]

    fn roundtrip_repartition() -> Result<()> {
        use datafusion::logical_plan::Partitioning;

        let test_batch_sizes = [usize::MIN, usize::MAX, 43256];

        let test_expr: Vec<Expr> = vec![
            Expr::Column("c1".to_string()) + Expr::Column("c2".to_string()),
            Expr::Literal((4.0).into()),
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = std::sync::Arc::new(
            LogicalPlanBuilder::scan_csv(
                "employee.csv",
                CsvReadOptions::new().schema(&schema).has_header(true),
                Some(vec![3, 4]),
            )
            .and_then(|plan| plan.sort(&[col("salary")]))
            .and_then(|plan| plan.build())
            .map_err(BallistaError::DataFusionError)?,
        );

        for batch_size in test_batch_sizes.iter() {
            let rr_repartition = Partitioning::RoundRobinBatch(*batch_size);

            let roundtrip_plan = LogicalPlan::Repartition {
                input: plan.clone(),
                partitioning_scheme: rr_repartition,
            };

            roundtrip_test!(roundtrip_plan);

            let h_repartition = Partitioning::Hash(test_expr.clone(), *batch_size);

            let roundtrip_plan = LogicalPlan::Repartition {
                input: plan.clone(),
                partitioning_scheme: h_repartition,
            };

            roundtrip_test!(roundtrip_plan);

            let no_expr_hrepartition = Partitioning::Hash(Vec::new(), *batch_size);

            let roundtrip_plan = LogicalPlan::Repartition {
                input: plan.clone(),
                partitioning_scheme: no_expr_hrepartition,
            };

            roundtrip_test!(roundtrip_plan);
        }

        Ok(())
    }

    fn new_box_field(name: &str, dt: DataType, nullable: bool) -> Box<arrow::datatypes::Field> {
        Box::new(arrow::datatypes::Field::new(name, dt, nullable))
    }

    #[test]
    fn scalar_values_error_serialization() -> Result<()> {
        use arrow::datatypes::DataType;
        use datafusion::scalar::ScalarValue;
        let should_fail_on_seralize: Vec<ScalarValue> = vec![
            //Should fail due to inconsistent types
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Int16(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                DataType::List(new_box_field("item", DataType::Int16, true)),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                DataType::List(new_box_field("item", DataType::Int16, true)),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::List(
                        None,
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                    ScalarValue::List(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                    ScalarValue::List(
                        None,
                        DataType::List(new_box_field(
                            "lists are typed inconsistently",
                            DataType::Int16,
                            true,
                        )),
                    ),
                ]),
                DataType::List(new_box_field(
                    "level1",
                    DataType::List(new_box_field("level2", DataType::Float32, true)),
                    true,
                )),
            ),
        ];

        for test_case in should_fail_on_seralize.into_iter() {
            let res: Result<protobuf::ScalarValue> = (&test_case).try_into();
            if let Ok(val) = res {
                return Err(BallistaError::General(format!(
                    "The value {:?} should not have been able to serialize. Serialized to :{:?}",
                    test_case, val
                )));
            }
        }
        Ok(())
    }

    #[test]
    fn round_trip_scalar_values() -> Result<()> {
        use arrow::datatypes::DataType;
        use datafusion::scalar::ScalarValue;
        let should_pass: Vec<ScalarValue> = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::List(None, DataType::Boolean),
            ScalarValue::Date32(None),
            ScalarValue::TimeMicrosecond(None),
            ScalarValue::TimeNanosecond(None),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(f32::MAX)),
            ScalarValue::Float32(Some(f32::MIN)),
            ScalarValue::Float32(Some(-2000.0)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(f64::MAX)),
            ScalarValue::Float64(Some(f64::MIN)),
            ScalarValue::Float64(Some(-2000.0)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int8(Some(-15)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int16(Some(-15)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(-15)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(-15)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(0)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(0)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(0)),
            ScalarValue::Utf8(Some(String::from("Test string   "))),
            ScalarValue::LargeUtf8(Some(String::from("Test Large utf8"))),
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(i32::MAX)),
            ScalarValue::TimeNanosecond(Some(0)),
            ScalarValue::TimeNanosecond(Some(i64::MAX)),
            ScalarValue::TimeMicrosecond(Some(0)),
            ScalarValue::TimeMicrosecond(Some(i64::MAX)),
            ScalarValue::TimeMicrosecond(None),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Float32(Some(-213.1)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(5.5)),
                    ScalarValue::Float32(Some(2.0)),
                    ScalarValue::Float32(Some(1.0)),
                ]),
                DataType::List(new_box_field("level1", DataType::Float32, true)),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::List(
                        None,
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                    ScalarValue::List(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                ]),
                DataType::List(new_box_field(
                    "level1",
                    DataType::List(new_box_field("level2", DataType::Float32, true)),
                    true,
                )),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let proto: protobuf::ScalarValue = (&test_case).try_into()?;
            let _roundtrip: ScalarValue = (&proto).try_into()?;
        }

        Ok(())
    }

    #[test]
    fn round_trip_scalar_types() -> Result<()> {
        use arrow::datatypes::DataType;
        use arrow::datatypes::{IntervalUnit, TimeUnit};
        let should_pass: Vec<DataType> = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Utf8,
            DataType::LargeUtf8,
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Boolean, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field("Level2", DataType::Date32, true)),
                true,
            )),
        ];

        let should_fail: Vec<DataType> = vec![
            DataType::Null,
            DataType::Float16,
            //Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Decimal(1345, 5431),
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            //Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            //Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Union(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let proto: protobuf::ScalarType = (&test_case).try_into()?;
            let roundtrip: DataType = (&proto).try_into()?;
            assert_eq!(format!("{:?}", test_case), format!("{:?}", roundtrip));
        }

        let mut success: Vec<DataType> = Vec::new();
        for test_case in should_fail.into_iter() {
            let proto: Result<protobuf::ScalarType> = (&test_case).try_into();
            if proto.is_ok() {
                success.push(test_case)
            }
        }
        if !success.is_empty() {
            return Err(BallistaError::General(format!(
                "The following items which should have ressulted in an error completed successfully: {:?}",
                success
            )));
        }
        Ok(())
    }

    #[test]
    fn round_trip_datatype() -> Result<()> {
        use arrow::datatypes::DataType;
        use arrow::datatypes::{IntervalUnit, TimeUnit};
        let test_cases: Vec<DataType> = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            //Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Decimal(1345, 5431),
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            //Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            //Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Union(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in test_cases.into_iter() {
            let proto: protobuf::ArrowType = (&test_case).into();
            let roundtrip: DataType = (&proto).try_into()?;
            assert_eq!(format!("{:?}", test_case), format!("{:?}", roundtrip));
        }
        Ok(())
    }

    #[test]
    fn roundtrip_null_scalar_values() -> Result<()> {
        use arrow::datatypes::DataType;
        use arrow::datatypes::Field;
        use datafusion::scalar::ScalarValue;
        let test_types = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Date32(None),
            ScalarValue::TimeMicrosecond(None),
            ScalarValue::TimeNanosecond(None),
            //ScalarValue::List(None, DataType::Boolean)
        ];

        for test_case in test_types.into_iter() {
            let proto_scalar: protobuf::ScalarValue = (&test_case).try_into()?;
            let returned_scalar: datafusion::scalar::ScalarValue = (&proto_scalar).try_into()?;
            assert_eq!(
                format!("{:?}", &test_case),
                format!("{:?}", returned_scalar)
            );
        }

        Ok(())
    }

    #[test]

    fn roundtrip_create_external_table() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        use datafusion::logical_plan::ToDFSchema;

        let df_schema_ref = schema.to_dfschema_ref()?;

        use datafusion::sql::parser::FileType;

        let filetypes: [FileType; 3] = [FileType::NdJson, FileType::Parquet, FileType::CSV];

        for file in filetypes.iter() {
            let create_table_node = LogicalPlan::CreateExternalTable {
                schema: df_schema_ref.clone(),
                name: String::from("TestName"),
                location: String::from("employee.csv"),
                file_type: *file,
                has_header: true,
            };

            roundtrip_test!(create_table_node);
        }

        Ok(())
    }

    #[test]

    fn roundtrip_explain() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let verbose_plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.sort(&[col("salary")]))
        .and_then(|plan| plan.explain(true))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.sort(&[col("salary")]))
        .and_then(|plan| plan.explain(false))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);

        roundtrip_test!(verbose_plan);

        Ok(())
    }

    #[test]
    fn roundtrip_join() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let scan_plan = LogicalPlanBuilder::empty(false)
            .build()
            .map_err(BallistaError::DataFusionError)?;
        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.join(&scan_plan, JoinType::Inner, &["id"], &["id"]))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);
        Ok(())
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.sort(&[col("salary")]))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;
        roundtrip_test!(plan);

        Ok(())
    }

    #[test]

    fn roundtrip_empty_relation() -> Result<()> {
        let plan_false = LogicalPlanBuilder::empty(false)
            .build()
            .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan_false);

        let plan_true = LogicalPlanBuilder::empty(true)
            .build()
            .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan_true);

        Ok(())
    }

    #[test]

    fn roundtrip_logical_plan() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.aggregate(&[col("state")], &[max(col("salary"))]))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);

        Ok(())
    }

    #[test]

    fn roundtrip_not() -> Result<()> {
        let test_expr = Expr::Not(Box::new(Expr::Literal((1.0).into())));

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_is_null() -> Result<()> {
        let test_expr = Expr::IsNull(Box::new(Expr::Column("id".into())));

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_is_not_null() -> Result<()> {
        let test_expr = Expr::IsNotNull(Box::new(Expr::Column("id".into())));

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_between() -> Result<()> {
        let test_expr = Expr::Between {
            expr: Box::new(Expr::Literal((1.0).into())),
            negated: true,
            low: Box::new(Expr::Literal((2.0).into())),
            high: Box::new(Expr::Literal((3.0).into())),
        };

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_case() -> Result<()> {
        let test_expr = Expr::Case {
            expr: Some(Box::new(Expr::Literal((1.0).into()))),
            when_then_expr: vec![(
                Box::new(Expr::Literal((2.0).into())),
                Box::new(Expr::Literal((3.0).into())),
            )],
            else_expr: Some(Box::new(Expr::Literal((4.0).into()))),
        };

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_cast() -> Result<()> {
        let test_expr = Expr::Cast {
            expr: Box::new(Expr::Literal((1.0).into())),
            data_type: DataType::Boolean,
        };

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_sort_expr() -> Result<()> {
        let test_expr = Expr::Sort {
            expr: Box::new(Expr::Literal((1.0).into())),
            asc: true,
            nulls_first: true,
        };

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_negative() -> Result<()> {
        let test_expr = Expr::Negative(Box::new(Expr::Literal((1.0).into())));

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_inlist() -> Result<()> {
        let test_expr = Expr::InList {
            expr: Box::new(Expr::Literal((1.0).into())),
            list: vec![Expr::Literal((2.0).into())],
            negated: true,
        };

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]

    fn roundtrip_wildcard() -> Result<()> {
        let test_expr = Expr::Wildcard;

        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }

    #[test]
    fn roundtrip_sqrt() -> Result<()> {
        let test_expr = Expr::ScalarFunction {
            fun: Sqrt,
            args: vec![col("col")],
        };
        roundtrip_test!(test_expr, protobuf::LogicalExprNode, Expr);

        Ok(())
    }
}
