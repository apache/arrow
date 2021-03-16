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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::{
    convert::{From, TryInto},
    unimplemented,
};

use crate::error::BallistaError;
use crate::serde::{proto_error, protobuf};
use crate::{convert_box_required, convert_required};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_plan::{
    abs, acos, asin, atan, ceil, cos, exp, floor, log10, log2, round, signum, sin, sqrt, tan,
    trunc, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::scalar::ScalarValue;
use protobuf::logical_plan_node::LogicalPlanType;
use protobuf::{logical_expr_node::ExprType, scalar_type};

// use uuid::Uuid;

impl TryInto<LogicalPlan> for &protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        let plan = self.logical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            LogicalPlanType::Projection(projection) => {
                let input: LogicalPlan = convert_box_required!(projection.input)?;
                LogicalPlanBuilder::from(&input)
                    .project(
                        &projection
                            .expr
                            .iter()
                            .map(|expr| expr.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Selection(selection) => {
                let input: LogicalPlan = convert_box_required!(selection.input)?;
                LogicalPlanBuilder::from(&input)
                    .filter(
                        selection
                            .expr
                            .as_ref()
                            .expect("expression required")
                            .try_into()?,
                    )?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input: LogicalPlan = convert_box_required!(aggregate.input)?;
                let group_expr = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let aggr_expr = aggregate
                    .aggr_expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(&input)
                    .aggregate(&group_expr, &aggr_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::CsvScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;
                let options = CsvReadOptions::new()
                    .schema(&schema)
                    .delimiter(scan.delimiter.as_bytes()[0])
                    .file_extension(&scan.file_extension)
                    .has_header(scan.has_header);

                let mut projection = None;
                if let Some(column_names) = &scan.projection {
                    let column_indices = column_names
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                LogicalPlanBuilder::scan_csv(&scan.path, options, projection)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::ParquetScan(scan) => {
                let projection = match scan.projection.as_ref() {
                    None => None,
                    Some(columns) => {
                        let schema: Schema = convert_required!(scan.schema)?;
                        let r: Result<Vec<usize>, _> = columns
                            .columns
                            .iter()
                            .map(|col_name| {
                                schema.fields().iter().position(|field| field.name() == col_name).ok_or_else(|| {
                                    let column_names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
                                    proto_error(format!(
                                        "Parquet projection contains column name that is not present in schema. Column name: {}. Schema columns: {:?}",
                                        col_name, column_names
                                    ))
                                })
                            })
                            .collect();
                        Some(r?)
                    }
                };
                LogicalPlanBuilder::scan_parquet(&scan.path, projection, 24)? //TODO concurrency
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Sort(sort) => {
                let input: LogicalPlan = convert_box_required!(sort.input)?;
                let sort_expr: Vec<Expr> = sort
                    .expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(&input)
                    .sort(&sort_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Repartition(repartition) => {
                use datafusion::logical_plan::Partitioning;
                let input: LogicalPlan = convert_box_required!(repartition.input)?;
                use protobuf::repartition_node::PartitionMethod;
                let pb_partition_method = repartition.partition_method.clone().ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, RepartitionNode was missing required field 'partition_method'",
                    ))
                })?;

                let partitioning_scheme = match pb_partition_method {
                    PartitionMethod::Hash(protobuf::HashRepartition {
                        hash_expr: pb_hash_expr,
                        partition_count,
                    }) => Partitioning::Hash(
                        pb_hash_expr
                            .iter()
                            .map(|pb_expr| pb_expr.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                        partition_count as usize,
                    ),
                    PartitionMethod::RoundRobin(batch_size) => {
                        Partitioning::RoundRobinBatch(batch_size as usize)
                    }
                };

                LogicalPlanBuilder::from(&input)
                    .repartition(partitioning_scheme)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::EmptyRelation(empty_relation) => {
                LogicalPlanBuilder::empty(empty_relation.produce_one_row)
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::CreateExternalTable(create_extern_table) => {
                let pb_schema = (create_extern_table.schema.clone()).ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required field schema.",
                    ))
                })?;

                let pb_file_type: protobuf::FileType = create_extern_table.file_type.try_into()?;

                Ok(LogicalPlan::CreateExternalTable {
                    schema: pb_schema.try_into()?,
                    name: create_extern_table.name.clone(),
                    location: create_extern_table.location.clone(),
                    file_type: pb_file_type.into(),
                    has_header: create_extern_table.has_header,
                })
            }
            LogicalPlanType::Explain(explain) => {
                let input: LogicalPlan = convert_box_required!(explain.input)?;
                LogicalPlanBuilder::from(&input)
                    .explain(explain.verbose)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Limit(limit) => {
                let input: LogicalPlan = convert_box_required!(limit.input)?;
                LogicalPlanBuilder::from(&input)
                    .limit(limit.limit as usize)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Join(join) => {
                let left_keys: Vec<&str> =
                    join.left_join_column.iter().map(|i| i.as_str()).collect();
                let right_keys: Vec<&str> =
                    join.right_join_column.iter().map(|i| i.as_str()).collect();
                let join_type = protobuf::JoinType::from_i32(join.join_type).ok_or_else(|| {
                    proto_error(format!(
                        "Received a JoinNode message with unknown JoinType {}",
                        join.join_type
                    ))
                })?;
                let join_type = match join_type {
                    protobuf::JoinType::Inner => JoinType::Inner,
                    protobuf::JoinType::Left => JoinType::Left,
                    protobuf::JoinType::Right => JoinType::Right,
                };
                LogicalPlanBuilder::from(&convert_box_required!(join.left)?)
                    .join(
                        &convert_box_required!(join.right)?,
                        join_type,
                        &left_keys,
                        &right_keys,
                    )?
                    .build()
                    .map_err(|e| e.into())
            }
        }
    }
}

impl TryInto<datafusion::logical_plan::DFSchema> for protobuf::Schema {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::logical_plan::DFSchema, Self::Error> {
        let schema: Schema = (&self).try_into()?;
        schema.try_into().map_err(BallistaError::DataFusionError)
    }
}

impl TryInto<datafusion::logical_plan::DFSchemaRef> for protobuf::Schema {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::logical_plan::DFSchemaRef, Self::Error> {
        use datafusion::logical_plan::ToDFSchema;
        let schema: Schema = (&self).try_into()?;
        schema
            .to_dfschema_ref()
            .map_err(BallistaError::DataFusionError)
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::scalar_type::Datatype {
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        use protobuf::scalar_type::Datatype;
        Ok(match self {
            Datatype::Scalar(scalar_type) => {
                let pb_scalar_enum = protobuf::PrimitiveScalarType::from_i32(*scalar_type).ok_or_else(|| {
                    proto_error(format!(
                        "Protobuf deserialization error, scalar_type::Datatype missing was provided invalid enum variant: {}",
                        *scalar_type
                    ))
                })?;
                pb_scalar_enum.into()
            }
            Datatype::List(protobuf::ScalarListType {
                deepest_type,
                field_names,
            }) => {
                if field_names.is_empty() {
                    return Err(proto_error(
                        "Protobuf deserialization error: found no field names in ScalarListType message which requires at least one",
                    ));
                }
                let pb_scalar_type = protobuf::PrimitiveScalarType::from_i32(*deepest_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Protobuf deserialization error: invalid i32 for scalar enum: {}",
                            *deepest_type
                        ))
                    })?;
                //Because length is checked above it is safe to unwrap .last()
                let mut scalar_type = arrow::datatypes::DataType::List(Box::new(Field::new(
                    field_names.last().unwrap().as_str(),
                    pb_scalar_type.into(),
                    true,
                )));
                //Iterate over field names in reverse order except for the last item in the vector
                for name in field_names.iter().rev().skip(1) {
                    let new_datatype = arrow::datatypes::DataType::List(Box::new(Field::new(
                        name.as_str(),
                        scalar_type,
                        true,
                    )));
                    scalar_type = new_datatype;
                }
                scalar_type
            }
        })
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::arrow_type::ArrowTypeEnum {
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        use arrow::datatypes::DataType;
        use protobuf::arrow_type;
        Ok(match self {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal { whole, fractional }) => {
                DataType::Decimal(*whole as usize, *fractional as usize)
            }
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type.try_into()?), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => DataType::Union(
                union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let pb_key_datatype = dict
                    .as_ref()
                    .key
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let pb_value_datatype = dict
                    .as_ref()
                    .value
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let key_datatype: DataType = pb_key_datatype.as_ref().try_into()?;
                let value_datatype: DataType = pb_value_datatype.as_ref().try_into()?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}

impl Into<arrow::datatypes::DataType> for protobuf::PrimitiveScalarType {
    fn into(self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match self {
            protobuf::PrimitiveScalarType::Bool => DataType::Boolean,
            protobuf::PrimitiveScalarType::Uint8 => DataType::UInt8,
            protobuf::PrimitiveScalarType::Int8 => DataType::Int8,
            protobuf::PrimitiveScalarType::Uint16 => DataType::UInt16,
            protobuf::PrimitiveScalarType::Int16 => DataType::Int16,
            protobuf::PrimitiveScalarType::Uint32 => DataType::UInt32,
            protobuf::PrimitiveScalarType::Int32 => DataType::Int32,
            protobuf::PrimitiveScalarType::Uint64 => DataType::UInt64,
            protobuf::PrimitiveScalarType::Int64 => DataType::Int64,
            protobuf::PrimitiveScalarType::Float32 => DataType::Float32,
            protobuf::PrimitiveScalarType::Float64 => DataType::Float64,
            protobuf::PrimitiveScalarType::Utf8 => DataType::Utf8,
            protobuf::PrimitiveScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::PrimitiveScalarType::Date32 => DataType::Date32,
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
            }
            protobuf::PrimitiveScalarType::Null => DataType::Null,
        }
    }
}

//Does not typecheck lists
fn typechecked_scalar_value_conversion(
    tested_type: &protobuf::scalar_value::Value,
    required_type: protobuf::PrimitiveScalarType,
) -> Result<datafusion::scalar::ScalarValue, BallistaError> {
    use protobuf::scalar_value::Value;
    use protobuf::PrimitiveScalarType;
    Ok(match (tested_type, &required_type) {
        (Value::BoolValue(v), PrimitiveScalarType::Bool) => ScalarValue::Boolean(Some(*v)),
        (Value::Int8Value(v), PrimitiveScalarType::Int8) => ScalarValue::Int8(Some(*v as i8)),
        (Value::Int16Value(v), PrimitiveScalarType::Int16) => ScalarValue::Int16(Some(*v as i16)),
        (Value::Int32Value(v), PrimitiveScalarType::Int32) => ScalarValue::Int32(Some(*v)),
        (Value::Int64Value(v), PrimitiveScalarType::Int64) => ScalarValue::Int64(Some(*v)),
        (Value::Uint8Value(v), PrimitiveScalarType::Uint8) => ScalarValue::UInt8(Some(*v as u8)),
        (Value::Uint16Value(v), PrimitiveScalarType::Uint16) => {
            ScalarValue::UInt16(Some(*v as u16))
        }
        (Value::Uint32Value(v), PrimitiveScalarType::Uint32) => ScalarValue::UInt32(Some(*v)),
        (Value::Uint64Value(v), PrimitiveScalarType::Uint64) => ScalarValue::UInt64(Some(*v)),
        (Value::Float32Value(v), PrimitiveScalarType::Float32) => ScalarValue::Float32(Some(*v)),
        (Value::Float64Value(v), PrimitiveScalarType::Float64) => ScalarValue::Float64(Some(*v)),
        (Value::Date32Value(v), PrimitiveScalarType::Date32) => ScalarValue::Date32(Some(*v)),
        (Value::TimeMicrosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimeMicrosecond(Some(*v))
        }
        (Value::TimeNanosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimeNanosecond(Some(*v))
        }
        (Value::Utf8Value(v), PrimitiveScalarType::Utf8) => ScalarValue::Utf8(Some(v.to_owned())),
        (Value::LargeUtf8Value(v), PrimitiveScalarType::LargeUtf8) => {
            ScalarValue::LargeUtf8(Some(v.to_owned()))
        }

        (Value::NullValue(i32_enum), required_scalar_type) => {
            if *i32_enum == *required_scalar_type as i32 {
                let pb_scalar_type = PrimitiveScalarType::from_i32(*i32_enum).ok_or_else(|| {
                    BallistaError::General(format!(
                        "Invalid i32_enum={} when converting with PrimitiveScalarType::from_i32()",
                        *i32_enum
                    ))
                })?;
                let scalar_value: ScalarValue = match pb_scalar_type {
                    PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
                    PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
                    PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
                    PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
                    PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
                    PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
                    PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
                    PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
                    PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
                    PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
                    PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
                    PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
                    PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                    PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
                    PrimitiveScalarType::TimeMicrosecond => ScalarValue::TimeMicrosecond(None),
                    PrimitiveScalarType::TimeNanosecond => ScalarValue::TimeNanosecond(None),
                    PrimitiveScalarType::Null => {
                        return Err(proto_error(
                            "Untyped scalar null is not a valid scalar value",
                        ))
                    }
                };
                scalar_value
            } else {
                return Err(proto_error("Could not convert to the proper type"));
            }
        }
        _ => return Err(proto_error("Could not convert to the proper type")),
    })
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::scalar_value::Value {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        use protobuf::PrimitiveScalarType;
        let scalar = match self {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimeMicrosecond(Some(*v))
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimeNanosecond(Some(*v))
            }
            protobuf::scalar_value::Value::ListValue(v) => v.try_into()?,
            protobuf::scalar_value::Value::NullListValue(v) => {
                ScalarValue::List(None, v.try_into()?)
            }
            protobuf::scalar_value::Value::NullValue(null_enum) => {
                PrimitiveScalarType::from_i32(*null_enum)
                    .ok_or_else(|| proto_error("Invalid scalar type"))?
                    .try_into()?
            }
        };
        Ok(scalar)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarListValue {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use protobuf::scalar_type::Datatype;
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListValue { datatype, values } = self;
        let pb_scalar_type = datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue messsage missing required field 'datatype'"))?;
        let scalar_type = pb_scalar_type
            .datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue.Datatype messsage missing required field 'datatype'"))?;
        let scalar_values = match scalar_type {
            Datatype::Scalar(scalar_type_i32) => {
                let leaf_scalar_type = protobuf::PrimitiveScalarType::from_i32(*scalar_type_i32)
                    .ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = values
                    .iter()
                    .map(|protobuf::ScalarValue { value: opt_value }| {
                        let value = opt_value.as_ref().ok_or_else(|| {
                            proto_error(
                                "Protobuf deserialization error: missing required field 'value'",
                            )
                        })?;
                        typechecked_scalar_value_conversion(value, leaf_scalar_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                datafusion::scalar::ScalarValue::List(
                    Some(typechecked_values),
                    leaf_scalar_type.into(),
                )
            }
            Datatype::List(list_type) => {
                let protobuf::ScalarListType {
                    deepest_type,
                    field_names,
                } = &list_type;
                let leaf_type = PrimitiveScalarType::from_i32(*deepest_type)
                    .ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
                let depth = field_names.len();

                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = if depth == 0 {
                    return Err(proto_error(
                        "Protobuf deserialization error, ScalarListType had no field names, requires at least one",
                    ));
                } else if depth == 1 {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            typechecked_scalar_value_conversion(value, leaf_type)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            value.try_into()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                datafusion::scalar::ScalarValue::List(
                    match typechecked_values.len() {
                        0 => None,
                        _ => Some(typechecked_values),
                    },
                    list_type.try_into()?,
                )
            }
        };
        Ok(scalar_values)
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::ScalarListType {
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListType {
            deepest_type,
            field_names,
        } = self;

        let depth = field_names.len();
        if depth == 0 {
            return Err(proto_error(
                "Protobuf deserialization error: Found a ScalarListType message with no field names, at least one is required",
            ));
        }

        let mut curr_type = arrow::datatypes::DataType::List(Box::new(Field::new(
            //Since checked vector is not empty above this is safe to unwrap
            field_names.last().unwrap(),
            PrimitiveScalarType::from_i32(*deepest_type)
                .ok_or_else(|| proto_error("Could not convert to datafusion scalar type"))?
                .into(),
            true,
        )));
        //Iterates over field names in reverse order except for the last item in the vector
        for name in field_names.iter().rev().skip(1) {
            let temp_curr_type =
                arrow::datatypes::DataType::List(Box::new(Field::new(name, curr_type, true)));
            curr_type = temp_curr_type;
        }
        Ok(curr_type)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for protobuf::PrimitiveScalarType {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        Ok(match self {
            protobuf::PrimitiveScalarType::Null => {
                return Err(proto_error("Untyped null is an invalid scalar value"))
            }
            protobuf::PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
            protobuf::PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
            protobuf::PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
            protobuf::PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
            protobuf::PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
            protobuf::PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
            protobuf::PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
            protobuf::PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
            protobuf::PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
            protobuf::PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
            protobuf::PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
            protobuf::PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
            protobuf::PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            protobuf::PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
            protobuf::PrimitiveScalarType::TimeMicrosecond => ScalarValue::TimeMicrosecond(None),
            protobuf::PrimitiveScalarType::TimeNanosecond => ScalarValue::TimeNanosecond(None),
        })
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarValue {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        let value = self.value.as_ref().ok_or_else(|| {
            proto_error("Protobuf deserialization error: missing required field 'value'")
        })?;
        Ok(match value {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimeMicrosecond(Some(*v))
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimeNanosecond(Some(*v))
            }
            protobuf::scalar_value::Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue {
                    values,
                    datatype: opt_scalar_type,
                } = &scalar_list;
                let pb_scalar_type = opt_scalar_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization err: ScalaListValue missing required field 'datatype'"))?;
                let typechecked_values: Vec<ScalarValue> = values
                    .iter()
                    .map(|val| val.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let scalar_type: arrow::datatypes::DataType = pb_scalar_type.try_into()?;
                ScalarValue::List(Some(typechecked_values), scalar_type)
            }
            protobuf::scalar_value::Value::NullListValue(v) => {
                let pb_datatype = v
                    .datatype
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: NullListValue message missing required field 'datatyp'"))?;
                ScalarValue::List(None, pb_datatype.try_into()?)
            }
            protobuf::scalar_value::Value::NullValue(v) => {
                let null_type_enum = protobuf::PrimitiveScalarType::from_i32(*v)
                    .ok_or_else(|| proto_error("Protobuf deserialization error found invalid enum variant for DatafusionScalar"))?;
                null_type_enum.try_into()?
            }
        })
    }
}

impl TryInto<Expr> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        use protobuf::logical_expr_node::ExprType;

        let expr_type = self
            .expr_type
            .as_ref()
            .ok_or_else(|| proto_error("Unexpected empty logical expression"))?;
        match expr_type {
            ExprType::BinaryExpr(binary_expr) => Ok(Expr::BinaryExpr {
                left: Box::new(parse_required_expr(&binary_expr.l)?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(parse_required_expr(&binary_expr.r)?),
            }),
            ExprType::ColumnName(column_name) => Ok(Expr::Column(column_name.to_owned())),
            ExprType::Literal(literal) => {
                use datafusion::scalar::ScalarValue;
                let scalar_value: datafusion::scalar::ScalarValue = literal.try_into()?;
                Ok(Expr::Literal(scalar_value))
            }
            ExprType::AggregateExpr(expr) => {
                let aggr_function = protobuf::AggregateFunction::from_i32(expr.aggr_function)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown aggregate function: {}",
                            expr.aggr_function
                        ))
                    })?;
                let fun = match aggr_function {
                    protobuf::AggregateFunction::Min => AggregateFunction::Min,
                    protobuf::AggregateFunction::Max => AggregateFunction::Max,
                    protobuf::AggregateFunction::Sum => AggregateFunction::Sum,
                    protobuf::AggregateFunction::Avg => AggregateFunction::Avg,
                    protobuf::AggregateFunction::Count => AggregateFunction::Count,
                };

                Ok(Expr::AggregateFunction {
                    fun,
                    args: vec![parse_required_expr(&expr.expr)?],
                    distinct: false, //TODO
                })
            }
            ExprType::Alias(alias) => Ok(Expr::Alias(
                Box::new(parse_required_expr(&alias.expr)?),
                alias.alias.clone(),
            )),
            ExprType::IsNullExpr(is_null) => {
                Ok(Expr::IsNull(Box::new(parse_required_expr(&is_null.expr)?)))
            }
            ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
                parse_required_expr(&is_not_null.expr)?,
            ))),
            ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(&not.expr)?))),
            ExprType::Between(between) => Ok(Expr::Between {
                expr: Box::new(parse_required_expr(&between.expr)?),
                negated: between.negated,
                low: Box::new(parse_required_expr(&between.low)?),
                high: Box::new(parse_required_expr(&between.high)?),
            }),
            ExprType::Case(case) => {
                let when_then_expr = case
                    .when_then_expr
                    .iter()
                    .map(|e| {
                        Ok((
                            Box::new(match &e.when_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                            Box::new(match &e.then_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                        ))
                    })
                    .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, BallistaError>>()?;
                Ok(Expr::Case {
                    expr: parse_optional_expr(&case.expr)?.map(Box::new),
                    when_then_expr,
                    else_expr: parse_optional_expr(&case.else_expr)?.map(Box::new),
                })
            }
            ExprType::Cast(cast) => {
                let expr = Box::new(parse_required_expr(&cast.expr)?);
                let arrow_type: &protobuf::ArrowType = cast
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: CastNode message missing required field 'arrow_type'"))?;
                let data_type = arrow_type.try_into()?;
                Ok(Expr::Cast { expr, data_type })
            }
            ExprType::Sort(sort) => Ok(Expr::Sort {
                expr: Box::new(parse_required_expr(&sort.expr)?),
                asc: sort.asc,
                nulls_first: sort.nulls_first,
            }),
            ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(parse_required_expr(
                &negative.expr,
            )?))),
            ExprType::InList(in_list) => Ok(Expr::InList {
                expr: Box::new(parse_required_expr(&in_list.expr)?),
                list: in_list
                    .list
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                negated: in_list.negated,
            }),
            ExprType::Wildcard(_) => Ok(Expr::Wildcard),
            ExprType::ScalarFunction(expr) => {
                let scalar_function =
                    protobuf::ScalarFunction::from_i32(expr.fun).ok_or_else(|| {
                        proto_error(format!("Received an unknown scalar function: {}", expr.fun))
                    })?;
                match scalar_function {
                    protobuf::ScalarFunction::Sqrt => Ok(sqrt((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Sin => Ok(sin((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Cos => Ok(cos((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Tan => Ok(tan((&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Asin => Ok(asin(&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Acos => Ok(acos(&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Atan => Ok(atan((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Exp => Ok(exp((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Log2 => Ok(log2((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Log10 => Ok(log10((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Floor => Ok(floor((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Ceil => Ok(ceil((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Round => Ok(round((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Trunc => Ok(trunc((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Abs => Ok(abs((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Signum => Ok(signum((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Octetlength => {
                        Ok(length((&expr.expr[0]).try_into()?))
                    }
                    // // protobuf::ScalarFunction::Concat => Ok(concat((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Lower => Ok(lower((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Upper => Ok(upper((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Trim => Ok(trim((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Ltrim => Ok(ltrim((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Rtrim => Ok(rtrim((&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Totimestamp => Ok(to_timestamp((&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Array => Ok(array((&expr.expr[0]).try_into()?)),
                    // // protobuf::ScalarFunction::Nullif => Ok(nulli((&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Datetrunc => Ok(date_trunc((&expr.expr[0]).try_into()?)),
                    // protobuf::ScalarFunction::Md5 => Ok(md5((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Sha224 => Ok(sha224((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Sha256 => Ok(sha256((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Sha384 => Ok(sha384((&expr.expr[0]).try_into()?)),
                    protobuf::ScalarFunction::Sha512 => Ok(sha512((&expr.expr[0]).try_into()?)),
                    _ => Err(proto_error(
                        "Protobuf deserialization error: Unsupported scalar function",
                    )),
                }
            }
        }
    }
}

fn from_proto_binary_op(op: &str) -> Result<Operator, BallistaError> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Like" => Ok(Operator::Like),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::ScalarType {
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        let pb_scalartype = self
            .datatype
            .as_ref()
            .ok_or_else(|| proto_error("ScalarType message missing required field 'datatype'"))?;
        pb_scalartype.try_into()
    }
}

impl TryInto<Schema> for &protobuf::Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<Schema, BallistaError> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let pb_arrow_type_res = c
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Field message was missing required field 'arrow_type'"));
                let pb_arrow_type: &protobuf::ArrowType = match pb_arrow_type_res {
                    Ok(res) => res,
                    Err(e) => return Err(e),
                };
                Ok(Field::new(&c.name, pb_arrow_type.try_into()?, c.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

impl TryInto<arrow::datatypes::Field> for &protobuf::Field {
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::Field, Self::Error> {
        let pb_datatype = self.arrow_type.as_ref().ok_or_else(|| {
            proto_error(
                "Protobuf deserialization error: Field message missing required field 'arrow_type'",
            )
        })?;

        Ok(arrow::datatypes::Field::new(
            self.name.as_str(),
            pb_datatype.as_ref().try_into()?,
            self.nullable,
        ))
    }
}

use datafusion::physical_plan::datetime_expressions::{date_trunc, to_timestamp};
use datafusion::prelude::{
    array, length, lower, ltrim, md5, rtrim, sha224, sha256, sha384, sha512, trim, upper,
};
use std::convert::TryFrom;

impl TryFrom<i32> for protobuf::FileType {
    type Error = BallistaError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use protobuf::FileType;
        match value {
            _x if _x == FileType::NdJson as i32 => Ok(FileType::NdJson),
            _x if _x == FileType::Parquet as i32 => Ok(FileType::Parquet),
            _x if _x == FileType::Csv as i32 => Ok(FileType::Csv),
            invalid => Err(BallistaError::General(format!(
                "Attempted to convert invalid i32 to protobuf::Filetype: {}",
                invalid
            ))),
        }
    }
}

impl Into<datafusion::sql::parser::FileType> for protobuf::FileType {
    fn into(self) -> datafusion::sql::parser::FileType {
        use datafusion::sql::parser::FileType;
        match self {
            protobuf::FileType::NdJson => FileType::NdJson,
            protobuf::FileType::Parquet => FileType::Parquet,
            protobuf::FileType::Csv => FileType::CSV,
        }
    }
}

fn parse_required_expr(p: &Option<Box<protobuf::LogicalExprNode>>) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into(),
        None => Err(proto_error("Missing required expression")),
    }
}

fn parse_optional_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Option<Expr>, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into().map(Some),
        None => Ok(None),
    }
}
