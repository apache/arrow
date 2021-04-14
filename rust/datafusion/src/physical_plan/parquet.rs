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

//! Execution plan for reading Parquet files

use std::fmt;
use std::fs::File;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use super::{
    planner::DefaultPhysicalPlanner, ColumnarValue, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream,
};
use crate::{
    catalog::catalog::MemoryCatalogList,
    physical_plan::{common, ExecutionPlan, Partitioning},
};
use crate::{
    error::{DataFusionError, Result},
    execution::context::ExecutionContextState,
    logical_plan::{Expr, Operator},
    optimizer::utils,
    prelude::ExecutionConfig,
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::new_null_array,
    error::{ArrowError, Result as ArrowResult},
};
use arrow::{
    array::{make_array, ArrayData, ArrayRef, BooleanArray, BooleanBufferBuilder},
    buffer::MutableBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use parquet::file::{
    metadata::RowGroupMetaData,
    reader::{FileReader, SerializedFileReader},
    statistics::Statistics as ParquetStatistics,
};

use fmt::Debug;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::datasource::datasource::{ColumnStatistics, Statistics};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Parquet partitions to read
    partitions: Vec<ParquetPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
    /// Statistics for the data set (sum of statistics for all partitions)
    statistics: Statistics,
    /// Optional predicate builder
    predicate_builder: Option<RowGroupPredicateBuilder>,
    /// Optional limit of the number of rows
    limit: Option<usize>,
}

/// Represents one partition of a Parquet data set and this currently means one Parquet file.
///
/// In the future it would be good to support subsets of files based on ranges of row groups
/// so that we can better parallelize reads of large files across available cores (see
/// [ARROW-10995](https://issues.apache.org/jira/browse/ARROW-10995)).
///
/// We may also want to support reading Parquet files that are partitioned based on a key and
/// in this case we would want this partition struct to represent multiple files for a given
/// partition key (see [ARROW-11019](https://issues.apache.org/jira/browse/ARROW-11019)).
#[derive(Debug, Clone)]
pub struct ParquetPartition {
    /// The Parquet filename for this partition
    pub filenames: Vec<String>,
    /// Statistics for this partition
    pub statistics: Statistics,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan based on the specified Parquet filename or
    /// directory containing Parquet files
    pub fn try_from_path(
        path: &str,
        projection: Option<Vec<usize>>,
        predicate: Option<Expr>,
        batch_size: usize,
        max_concurrency: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        // build a list of filenames from the specified path, which could be a single file or
        // a directory containing one or more parquet files
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        if filenames.is_empty() {
            Err(DataFusionError::Plan(format!(
                "No Parquet files found at path {}",
                path
            )))
        } else {
            let filenames = filenames
                .iter()
                .map(|filename| filename.as_str())
                .collect::<Vec<&str>>();
            Self::try_from_files(
                &filenames,
                projection,
                predicate,
                batch_size,
                max_concurrency,
                limit,
            )
        }
    }

    /// Create a new Parquet reader execution plan based on the specified list of Parquet
    /// files
    pub fn try_from_files(
        filenames: &[&str],
        projection: Option<Vec<usize>>,
        predicate: Option<Expr>,
        batch_size: usize,
        max_concurrency: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        // build a list of Parquet partitions with statistics and gather all unique schemas
        // used in this data set
        let mut schemas: Vec<Schema> = vec![];
        let mut partitions = Vec::with_capacity(max_concurrency);
        let filenames: Vec<String> = filenames.iter().map(|s| s.to_string()).collect();
        let chunks = split_files(&filenames, max_concurrency);
        let mut num_rows = 0;
        let mut total_byte_size = 0;
        let mut null_counts = Vec::new();
        let mut limit_exhausted = false;
        for chunk in chunks {
            let mut filenames: Vec<String> =
                chunk.iter().map(|x| x.to_string()).collect();
            let mut total_files = 0;
            for filename in &filenames {
                total_files += 1;
                let file = File::open(filename)?;
                let file_reader = Arc::new(SerializedFileReader::new(file)?);
                let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                let meta_data = arrow_reader.get_metadata();
                // collect all the unique schemas in this data set
                let schema = arrow_reader.get_schema()?;
                let num_fields = schema.fields().len();
                if schemas.is_empty() || schema != schemas[0] {
                    schemas.push(schema);
                    null_counts = vec![0; num_fields]
                }
                for row_group_meta in meta_data.row_groups() {
                    num_rows += row_group_meta.num_rows();
                    total_byte_size += row_group_meta.total_byte_size();

                    // Currently assumes every Parquet file has same schema
                    // https://issues.apache.org/jira/browse/ARROW-11017
                    let columns_null_counts = row_group_meta
                        .columns()
                        .iter()
                        .flat_map(|c| c.statistics().map(|stats| stats.null_count()));

                    for (i, cnt) in columns_null_counts.enumerate() {
                        null_counts[i] += cnt
                    }
                    if limit.map(|x| num_rows >= x as i64).unwrap_or(false) {
                        limit_exhausted = true;
                        break;
                    }
                }
            }

            let column_stats = null_counts
                .iter()
                .map(|null_count| ColumnStatistics {
                    null_count: Some(*null_count as usize),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                })
                .collect();

            let statistics = Statistics {
                num_rows: Some(num_rows as usize),
                total_byte_size: Some(total_byte_size as usize),
                column_statistics: Some(column_stats),
            };
            // remove files that are not needed in case of limit
            filenames.truncate(total_files);
            partitions.push(ParquetPartition {
                filenames,
                statistics,
            });
            if limit_exhausted {
                break;
            }
        }

        // we currently get the schema information from the first file rather than do
        // schema merging and this is a limitation.
        // See https://issues.apache.org/jira/browse/ARROW-11017
        if schemas.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "The Parquet files have {} different schemas and DataFusion does \
                not yet support schema merging",
                schemas.len()
            )));
        }
        let schema = schemas[0].clone();
        let predicate_builder = predicate.and_then(|predicate_expr| {
            RowGroupPredicateBuilder::try_new(&predicate_expr, schema.clone()).ok()
        });

        Ok(Self::new(
            partitions,
            schema,
            projection,
            predicate_builder,
            batch_size,
            limit,
        ))
    }

    /// Create a new Parquet reader execution plan with provided partitions and schema
    pub fn new(
        partitions: Vec<ParquetPartition>,
        schema: Schema,
        projection: Option<Vec<usize>>,
        predicate_builder: Option<RowGroupPredicateBuilder>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        // sum the statistics
        let mut num_rows: Option<usize> = None;
        let mut total_byte_size: Option<usize> = None;
        let mut null_counts: Vec<usize> = vec![0; schema.fields().len()];
        let mut has_null_counts = false;
        for part in &partitions {
            if let Some(n) = part.statistics.num_rows {
                num_rows = Some(num_rows.unwrap_or(0) + n)
            }
            if let Some(n) = part.statistics.total_byte_size {
                total_byte_size = Some(total_byte_size.unwrap_or(0) + n)
            }
            if let Some(x) = &part.statistics.column_statistics {
                let part_nulls: Vec<Option<usize>> =
                    x.iter().map(|c| c.null_count).collect();
                has_null_counts = true;

                for &i in projection.iter() {
                    null_counts[i] = part_nulls[i].unwrap_or(0);
                }
            }
        }
        let column_stats = if has_null_counts {
            Some(
                null_counts
                    .iter()
                    .map(|null_count| ColumnStatistics {
                        null_count: Some(*null_count),
                        distinct_count: None,
                        max_value: None,
                        min_value: None,
                    })
                    .collect(),
            )
        } else {
            None
        };

        let statistics = Statistics {
            num_rows,
            total_byte_size,
            column_statistics: column_stats,
        };
        Self {
            partitions,
            schema: Arc::new(projected_schema),
            projection,
            predicate_builder,
            batch_size,
            statistics,
            limit,
        }
    }

    /// Parquet partitions to read
    pub fn partitions(&self) -> &[ParquetPartition] {
        &self.partitions
    }

    /// Projection for which columns to load
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Statistics for the data set (sum of statistics for all partitions)
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

impl ParquetPartition {
    /// Create a new parquet partition
    pub fn new(filenames: Vec<String>, statistics: Statistics) -> Self {
        Self {
            filenames,
            statistics,
        }
    }

    /// The Parquet filename for this partition
    pub fn filenames(&self) -> &[String] {
        &self.filenames
    }

    /// Statistics for this partition
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

#[derive(Debug, Clone)]
/// Predicate builder used for generating of predicate functions, used to filter row group metadata
pub struct RowGroupPredicateBuilder {
    parquet_schema: Schema,
    predicate_expr: Arc<dyn PhysicalExpr>,
    stat_column_req: Vec<(String, StatisticsType, Field)>,
}

impl RowGroupPredicateBuilder {
    /// Try to create a new instance of PredicateExpressionBuilder.
    /// This will translate the filter expression into a statistics predicate expression
    /// (for example (column / 2) = 4 becomes (column_min / 2) <= 4 && 4 <= (column_max / 2)),
    /// then convert it to a DataFusion PhysicalExpression and cache it for later use by build_row_group_predicate.
    pub fn try_new(expr: &Expr, parquet_schema: Schema) -> Result<Self> {
        // build predicate expression once
        let mut stat_column_req = Vec::<(String, StatisticsType, Field)>::new();
        let logical_predicate_expr =
            build_predicate_expression(expr, &parquet_schema, &mut stat_column_req)?;
        // println!(
        //     "RowGroupPredicateBuilder::try_new, logical_predicate_expr: {:?}",
        //     logical_predicate_expr
        // );
        // build physical predicate expression
        let stat_fields = stat_column_req
            .iter()
            .map(|(_, _, f)| f.clone())
            .collect::<Vec<_>>();
        let stat_schema = Schema::new(stat_fields);
        let execution_context_state = ExecutionContextState {
            catalog_list: Arc::new(MemoryCatalogList::new()),
            scalar_functions: HashMap::new(),
            var_provider: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config: ExecutionConfig::new(),
        };
        let predicate_expr = DefaultPhysicalPlanner::default().create_physical_expr(
            &logical_predicate_expr,
            &stat_schema,
            &execution_context_state,
        )?;
        // println!(
        //     "RowGroupPredicateBuilder::try_new, predicate_expr: {:?}",
        //     predicate_expr
        // );
        Ok(Self {
            parquet_schema,
            predicate_expr,
            stat_column_req,
        })
    }

    /// Generate a predicate function used to filter row group metadata.
    /// This function takes a list of all row groups as parameter,
    /// so that DataFusion's physical expressions can be re-used by
    /// generating a RecordBatch, containing statistics arrays,
    /// on which the physical predicate expression is executed to generate a row group filter array.
    /// The generated filter array is then used in the returned closure to filter row groups.
    pub fn build_row_group_predicate(
        &self,
        row_group_metadata: &[RowGroupMetaData],
    ) -> Box<dyn Fn(&RowGroupMetaData, usize) -> bool> {
        // build statistics record batch
        let predicate_result = build_statistics_record_batch(
            row_group_metadata,
            &self.parquet_schema,
            &self.stat_column_req,
        )
        .and_then(|statistics_batch| {
            // execute predicate expression
            self.predicate_expr.evaluate(&statistics_batch)
        })
        .and_then(|v| match v {
            ColumnarValue::Array(array) => Ok(array),
            ColumnarValue::Scalar(_) => Err(DataFusionError::Plan(
                "predicate expression didn't return an array".to_string(),
            )),
        });

        let predicate_array = match predicate_result {
            Ok(array) => array,
            // row group filter array could not be built
            // return a closure which will not filter out any row groups
            _ => return Box::new(|_r, _i| true),
        };

        let predicate_array = predicate_array.as_any().downcast_ref::<BooleanArray>();
        match predicate_array {
            // return row group predicate function
            Some(array) => {
                // when the result of the predicate expression for a row group is null / undefined,
                // e.g. due to missing statistics, this row group can't be filtered out,
                // so replace with true
                let predicate_values =
                    array.iter().map(|x| x.unwrap_or(true)).collect::<Vec<_>>();
                Box::new(move |_, i| predicate_values[i])
            }
            // predicate result is not a BooleanArray
            // return a closure which will not filter out any row groups
            _ => Box::new(|_r, _i| true),
        }
    }
}

/// Build a RecordBatch from a list of RowGroupMetadata structs,
/// creating arrays, one for each statistics column,
/// as requested in the stat_column_req parameter.
fn build_statistics_record_batch(
    row_groups: &[RowGroupMetaData],
    parquet_schema: &Schema,
    stat_column_req: &[(String, StatisticsType, Field)],
) -> Result<RecordBatch> {
    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();
    for (column_name, statistics_type, stat_field) in stat_column_req {
        if let Some((column_index, _)) = parquet_schema.column_with_name(column_name) {
            let statistics = row_groups
                .iter()
                .map(|g| g.column(column_index).statistics())
                .collect::<Vec<_>>();
            let array = build_statistics_array(
                &statistics,
                *statistics_type,
                stat_field.data_type(),
            );
            fields.push(stat_field.clone());
            arrays.push(array);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Plan(err.to_string()))
}

struct StatisticsExpressionBuilder<'a> {
    column_name: String,
    column_expr: &'a Expr,
    scalar_expr: &'a Expr,
    parquet_field: &'a Field,
    stat_column_req: &'a mut Vec<(String, StatisticsType, Field)>,
    reverse_operator: bool,
}

impl<'a> StatisticsExpressionBuilder<'a> {
    fn try_new(
        left: &'a Expr,
        right: &'a Expr,
        parquet_schema: &'a Schema,
        stat_column_req: &'a mut Vec<(String, StatisticsType, Field)>,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let mut left_columns = HashSet::<String>::new();
        utils::expr_to_column_names(left, &mut left_columns)?;
        let mut right_columns = HashSet::<String>::new();
        utils::expr_to_column_names(right, &mut right_columns)?;
        let (column_expr, scalar_expr, column_names, reverse_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, false),
                (0, 1) => (right, left, right_columns, true),
                _ => {
                    // if more than one column used in expression - not supported
                    return Err(DataFusionError::Plan(
                        "Multi-column expressions are not currently supported"
                            .to_string(),
                    ));
                }
            };
        let column_name = column_names.iter().next().unwrap().clone();
        let field = match parquet_schema.column_with_name(&column_name) {
            Some((_, f)) => f,
            _ => {
                // field not found in parquet schema
                return Err(DataFusionError::Plan(
                    "Field not found in parquet schema".to_string(),
                ));
            }
        };

        Ok(Self {
            column_name,
            column_expr,
            scalar_expr,
            parquet_field: field,
            stat_column_req,
            reverse_operator,
        })
    }

    fn correct_operator(&self, op: Operator) -> Operator {
        if !self.reverse_operator {
            return op;
        }

        match op {
            Operator::Lt => Operator::Gt,
            Operator::Gt => Operator::Lt,
            Operator::LtEq => Operator::GtEq,
            Operator::GtEq => Operator::LtEq,
            _ => op,
        }
    }

    // fn column_expr(&self) -> &Expr {
    //     self.column_expr
    // }

    fn scalar_expr(&self) -> &Expr {
        self.scalar_expr
    }

    // fn column_name(&self) -> &String {
    //     &self.column_name
    // }

    fn is_stat_column_missing(&self, statistics_type: StatisticsType) -> bool {
        self.stat_column_req
            .iter()
            .filter(|(c, t, _f)| c == &self.column_name && t == &statistics_type)
            .count()
            == 0
    }

    fn stat_column_expr(
        &mut self,
        stat_type: StatisticsType,
        suffix: &str,
    ) -> Result<Expr> {
        let stat_column_name = format!("{}_{}", self.column_name, suffix);
        let stat_field = Field::new(
            stat_column_name.as_str(),
            self.parquet_field.data_type().clone(),
            self.parquet_field.is_nullable(),
        );
        if self.is_stat_column_missing(stat_type) {
            // only add statistics column if not previously added
            self.stat_column_req
                .push((self.column_name.clone(), stat_type, stat_field));
        }
        rewrite_column_expr(
            self.column_expr,
            self.column_name.as_str(),
            stat_column_name.as_str(),
        )
    }

    fn min_column_expr(&mut self) -> Result<Expr> {
        self.stat_column_expr(StatisticsType::Min, "min")
    }

    fn max_column_expr(&mut self) -> Result<Expr> {
        self.stat_column_expr(StatisticsType::Max, "max")
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(
    expr: &Expr,
    column_old_name: &str,
    column_new_name: &str,
) -> Result<Expr> {
    let expressions = utils::expr_sub_expressions(&expr)?;
    let expressions = expressions
        .iter()
        .map(|e| rewrite_column_expr(e, column_old_name, column_new_name))
        .collect::<Result<Vec<_>>>()?;

    if let Expr::Column(name) = expr {
        if name == column_old_name {
            return Ok(Expr::Column(column_new_name.to_string()));
        }
    }
    utils::rewrite_expression(&expr, &expressions)
}

/// Translate logical filter expression into parquet statistics predicate expression
fn build_predicate_expression(
    expr: &Expr,
    parquet_schema: &Schema,
    stat_column_req: &mut Vec<(String, StatisticsType, Field)>,
) -> Result<Expr> {
    use crate::logical_plan;
    // predicate expression can only be a binary expression
    let (left, op, right) = match expr {
        Expr::BinaryExpr { left, op, right } => (left, *op, right),
        _ => {
            // unsupported expression - replace with TRUE
            // this can still be useful when multiple conditions are joined using AND
            // such as: column > 10 AND TRUE
            return Ok(logical_plan::lit(true));
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr =
            build_predicate_expression(left, parquet_schema, stat_column_req)?;
        let right_expr =
            build_predicate_expression(right, parquet_schema, stat_column_req)?;
        return Ok(logical_plan::binary_expr(left_expr, op, right_expr));
    }

    let expr_builder = StatisticsExpressionBuilder::try_new(
        left,
        right,
        parquet_schema,
        stat_column_req,
    );
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(_) => {
            return Ok(logical_plan::lit(true));
        }
    };
    let corrected_op = expr_builder.correct_operator(op);
    let statistics_expr = match corrected_op {
        Operator::Eq => {
            // column = literal => (min, max) = literal => min <= literal && literal <= max
            // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            min_column_expr
                .lt_eq(expr_builder.scalar_expr().clone())
                .and(expr_builder.scalar_expr().clone().lt_eq(max_column_expr))
        }
        Operator::Gt => {
            // column > literal => (min, max) > literal => max > literal
            expr_builder
                .max_column_expr()?
                .gt(expr_builder.scalar_expr().clone())
        }
        Operator::GtEq => {
            // column >= literal => (min, max) >= literal => max >= literal
            expr_builder
                .max_column_expr()?
                .gt_eq(expr_builder.scalar_expr().clone())
        }
        Operator::Lt => {
            // column < literal => (min, max) < literal => min < literal
            expr_builder
                .min_column_expr()?
                .lt(expr_builder.scalar_expr().clone())
        }
        Operator::LtEq => {
            // column <= literal => (min, max) <= literal => min <= literal
            expr_builder
                .min_column_expr()?
                .lt_eq(expr_builder.scalar_expr().clone())
        }
        // other expressions are not supported
        _ => logical_plan::lit(true),
    };
    Ok(statistics_expr)
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum StatisticsType {
    Min,
    Max,
}

fn build_statistics_array(
    statistics: &[Option<&ParquetStatistics>],
    statistics_type: StatisticsType,
    data_type: &DataType,
) -> ArrayRef {
    let statistics_count = statistics.len();
    let first_group_stats = statistics.iter().find(|s| s.is_some());
    let first_group_stats = if let Some(Some(statistics)) = first_group_stats {
        // found first row group with statistics defined
        statistics
    } else {
        // no row group has statistics defined
        return new_null_array(data_type, statistics_count);
    };

    let (data_size, arrow_type) = match first_group_stats {
        ParquetStatistics::Int32(_) => (std::mem::size_of::<i32>(), DataType::Int32),
        ParquetStatistics::Int64(_) => (std::mem::size_of::<i64>(), DataType::Int64),
        ParquetStatistics::Float(_) => (std::mem::size_of::<f32>(), DataType::Float32),
        ParquetStatistics::Double(_) => (std::mem::size_of::<f64>(), DataType::Float64),
        ParquetStatistics::ByteArray(_) if data_type == &DataType::Utf8 => {
            (0, DataType::Utf8)
        }
        _ => {
            // type of statistics not supported
            return new_null_array(data_type, statistics_count);
        }
    };

    let statistics = statistics.iter().map(|s| {
        s.filter(|s| s.has_min_max_set())
            .map(|s| match statistics_type {
                StatisticsType::Min => s.min_bytes(),
                StatisticsType::Max => s.max_bytes(),
            })
    });

    if arrow_type == DataType::Utf8 {
        let data_size = statistics
            .clone()
            .map(|x| x.map(|b| b.len()).unwrap_or(0))
            .sum();
        let mut builder =
            arrow::array::StringBuilder::with_capacity(statistics_count, data_size);
        let string_statistics =
            statistics.map(|x| x.and_then(|bytes| std::str::from_utf8(bytes).ok()));
        for maybe_string in string_statistics {
            match maybe_string {
                Some(string_value) => builder.append_value(string_value).unwrap(),
                None => builder.append_null().unwrap(),
            };
        }
        return Arc::new(builder.finish());
    }

    let mut data_buffer = MutableBuffer::new(statistics_count * data_size);
    let mut bitmap_builder = BooleanBufferBuilder::new(statistics_count);
    let mut null_count = 0;
    for s in statistics {
        if let Some(stat_data) = s {
            bitmap_builder.append(true);
            data_buffer.extend_from_slice(stat_data);
        } else {
            bitmap_builder.append(false);
            data_buffer.resize(data_buffer.len() + data_size, 0);
            null_count += 1;
        }
    }

    let mut builder = ArrayData::builder(arrow_type)
        .len(statistics_count)
        .add_buffer(data_buffer.into());
    if null_count > 0 {
        builder = builder.null_bit_buffer(bitmap_builder.finish());
    }
    let array_data = builder.build();
    let statistics_array = make_array(array_data);
    if statistics_array.data_type() == data_type {
        return statistics_array;
    }
    // cast statistics array to required data type
    arrow::compute::cast(&statistics_array, data_type)
        .unwrap_or_else(|_| new_null_array(data_type, statistics_count))
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let filenames = self.partitions[partition].filenames.clone();
        let projection = self.projection.clone();
        let predicate_builder = self.predicate_builder.clone();
        let batch_size = self.batch_size;
        let limit = self.limit;

        task::spawn_blocking(move || {
            if let Err(e) = read_files(
                &filenames,
                &projection,
                &predicate_builder,
                batch_size,
                response_tx,
                limit,
            ) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream {
            schema: self.schema.clone(),
            inner: ReceiverStream::new(response_rx),
        }))
    }
}

fn send_result(
    response_tx: &Sender<ArrowResult<RecordBatch>>,
    result: ArrowResult<RecordBatch>,
) -> Result<()> {
    // Note this function is running on its own blockng tokio thread so blocking here is ok.
    response_tx
        .blocking_send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

fn read_files(
    filenames: &[String],
    projection: &[usize],
    predicate_builder: &Option<RowGroupPredicateBuilder>,
    batch_size: usize,
    response_tx: Sender<ArrowResult<RecordBatch>>,
    limit: Option<usize>,
) -> Result<()> {
    let mut total_rows = 0;
    'outer: for filename in filenames {
        let file = File::open(&filename)?;
        let mut file_reader = SerializedFileReader::new(file)?;
        if let Some(predicate_builder) = predicate_builder {
            let row_group_predicate = predicate_builder
                .build_row_group_predicate(file_reader.metadata().row_groups());
            file_reader.filter_row_groups(&row_group_predicate);
        }
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.to_owned(), batch_size)?;
        loop {
            match batch_reader.next() {
                Some(Ok(batch)) => {
                    //println!("ParquetExec got new batch from {}", filename);
                    total_rows += batch.num_rows();
                    send_result(&response_tx, Ok(batch))?;
                    if limit.map(|l| total_rows >= l).unwrap_or(false) {
                        break 'outer;
                    }
                }
                None => {
                    break;
                }
                Some(Err(e)) => {
                    let err_msg = format!(
                        "Error reading batch from {}: {}",
                        filename,
                        e.to_string()
                    );
                    // send error to operator
                    send_result(
                        &response_tx,
                        Err(ArrowError::ParquetError(err_msg.clone())),
                    )?;
                    // terminate thread with error
                    return Err(DataFusionError::Execution(err_msg));
                }
            }
        }
    }

    // finished reading files (dropping response_tx will close
    // channel)
    Ok(())
}

fn split_files(filenames: &[String], n: usize) -> Vec<&[String]> {
    let mut chunk_size = filenames.len() / n;
    if filenames.len() % n > 0 {
        chunk_size += 1;
    }
    filenames.chunks(chunk_size).collect()
}

struct ParquetStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use futures::StreamExt;
    use parquet::basic::Type as PhysicalType;
    use parquet::schema::types::SchemaDescPtr;

    #[test]
    fn test_split_files() {
        let filenames = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        let chunks = split_files(&filenames, 1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = split_files(&filenames, 2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = split_files(&filenames, 5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(&filenames, 123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let testdata = arrow::util::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::try_from_path(
            &filename,
            Some(vec![0, 1, 2]),
            None,
            1024,
            4,
            None,
        )?;
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[test]
    fn build_statistics_array_int32() {
        // build row group metadata array
        let s1 = ParquetStatistics::int32(None, Some(10), None, 0, false);
        let s2 = ParquetStatistics::int32(Some(2), Some(20), None, 0, false);
        let s3 = ParquetStatistics::int32(Some(3), Some(30), None, 0, false);
        let statistics = vec![Some(&s1), Some(&s2), Some(&s3)];

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &DataType::Int32);
        let int32_array = statistics_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let int32_vec = int32_array.into_iter().collect::<Vec<_>>();
        assert_eq!(int32_vec, vec![None, Some(2), Some(3)]);

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Max, &DataType::Int32);
        let int32_array = statistics_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let int32_vec = int32_array.into_iter().collect::<Vec<_>>();
        // here the first max value is None and not the Some(10) value which was actually set
        // because the min value is None
        assert_eq!(int32_vec, vec![None, Some(20), Some(30)]);
    }

    #[test]
    fn build_statistics_array_utf8() {
        // build row group metadata array
        let s1 = ParquetStatistics::byte_array(None, Some("10".into()), None, 0, false);
        let s2 = ParquetStatistics::byte_array(
            Some("2".into()),
            Some("20".into()),
            None,
            0,
            false,
        );
        let s3 = ParquetStatistics::byte_array(
            Some("3".into()),
            Some("30".into()),
            None,
            0,
            false,
        );
        let statistics = vec![Some(&s1), Some(&s2), Some(&s3)];

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &DataType::Utf8);
        let string_array = statistics_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let string_vec = string_array.into_iter().collect::<Vec<_>>();
        assert_eq!(string_vec, vec![None, Some("2"), Some("3")]);

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Max, &DataType::Utf8);
        let string_array = statistics_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let string_vec = string_array.into_iter().collect::<Vec<_>>();
        // here the first max value is None and not the Some("10") value which was actually set
        // because the min value is None
        assert_eq!(string_vec, vec![None, Some("20"), Some("30")]);
    }

    #[test]
    fn build_statistics_array_empty_stats() {
        let data_type = DataType::Int32;
        let statistics = vec![];
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), 0);

        let statistics = vec![None, None];
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), statistics.len());
        assert_eq!(statistics_array.data_type(), &data_type);
        for i in 0..statistics_array.len() {
            assert_eq!(statistics_array.is_null(i), true);
            assert_eq!(statistics_array.is_valid(i), false);
        }
    }

    #[test]
    fn build_statistics_array_unsupported_type() {
        // boolean is not currently a supported type for statistics
        let s1 = ParquetStatistics::boolean(Some(false), Some(true), None, 0, false);
        let s2 = ParquetStatistics::boolean(Some(false), Some(true), None, 0, false);
        let statistics = vec![Some(&s1), Some(&s2)];
        let data_type = DataType::Boolean;
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), statistics.len());
        assert_eq!(statistics_array.data_type(), &data_type);
        for i in 0..statistics_array.len() {
            assert_eq!(statistics_array.is_null(i), true);
            assert_eq!(statistics_array.is_valid(i), false);
        }
    }

    #[test]
    fn row_group_predicate_eq() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min LtEq Int32(1) And Int32(1) LtEq #c1_max";

        // test column on the left
        let expr = col("c1").eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max Gt Int32(1)";

        // test column on the left
        let expr = col("c1").gt(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).lt(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt_eq() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max GtEq Int32(1)";

        // test column on the left
        let expr = col("c1").gt_eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).lt_eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min Lt Int32(1)";

        // test column on the left
        let expr = col("c1").lt(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).gt(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_eq() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min LtEq Int32(1)";

        // test column on the left
        let expr = col("c1").lt_eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).gt_eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_and() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
            Field::new("c3", DataType::Int32, false),
        ]);
        // test AND operator joining supported c1 < 1 expression and unsupported c2 > c3 expression
        let expr = col("c1").lt(lit(1)).and(col("c2").lt(col("c3")));
        let expected_expr = "#c1_min Lt Int32(1) And Boolean(true)";
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_or() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test OR operator joining supported c1 < 1 expression and unsupported c2 % 2 expression
        let expr = col("c1").lt(lit(1)).or(col("c2").modulus(lit(2)));
        let expected_expr = "#c1_min Lt Int32(1) Or Boolean(true)";
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_stat_column_req() -> Result<()> {
        use crate::logical_plan::{col, lit};
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let mut stat_column_req = vec![];
        // c1 < 1 and (c2 = 2 or c2 = 3)
        let expr = col("c1")
            .lt(lit(1))
            .and(col("c2").eq(lit(2)).or(col("c2").eq(lit(3))));
        let expected_expr = "#c1_min Lt Int32(1) And #c2_min LtEq Int32(2) And Int32(2) LtEq #c2_max Or #c2_min LtEq Int32(3) And Int32(3) LtEq #c2_max";
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut stat_column_req)?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // c1 < 1 should add c1_min
        let c1_min_field = Field::new("c1_min", DataType::Int32, false);
        assert_eq!(
            stat_column_req[0],
            ("c1".to_owned(), StatisticsType::Min, c1_min_field)
        );
        // c2 = 2 should add c2_min and c2_max
        let c2_min_field = Field::new("c2_min", DataType::Int32, false);
        assert_eq!(
            stat_column_req[1],
            ("c2".to_owned(), StatisticsType::Min, c2_min_field)
        );
        let c2_max_field = Field::new("c2_max", DataType::Int32, false);
        assert_eq!(
            stat_column_req[2],
            ("c2".to_owned(), StatisticsType::Max, c2_max_field)
        );
        // c2 = 3 shouldn't add any new statistics fields
        assert_eq!(stat_column_req.len(), 3);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_simple_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let predicate_builder = RowGroupPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(1), Some(10), None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_row_group_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_missing_stats() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let predicate_builder = RowGroupPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(None, None, None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_row_group_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // missing statistics for first row group mean that the result from the predicate expression
        // is null / undefined so the first row group can't be filtered out
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_partial_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with partially supported expression
        // int > 1 and int % 2 => c1_max > 1 and true
        let expr = col("c1").gt(lit(15)).and(col("c2").modulus(lit(2)));
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let predicate_builder = RowGroupPredicateBuilder::try_new(&expr, schema.clone())?;

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::INT32),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_row_group_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(row_group_filter, vec![false, true]);

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").modulus(lit(2)));
        let predicate_builder = RowGroupPredicateBuilder::try_new(&expr, schema)?;
        let row_group_predicate =
            predicate_builder.build_row_group_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_unsupported_type() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with unsupported statistics type (boolean)
        // where a null array is generated for some statistics columns
        // int > 1 and bool = true => c1_max > 1 and null
        let expr = col("c1").gt(lit(15)).and(col("c2").eq(lit(true)));
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]);
        let predicate_builder = RowGroupPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::BOOLEAN),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_row_group_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // no row group is filtered out because the predicate expression can't be evaluated
        // when a null array is generated for a statistics column,
        // because the null values propagate to the end result, making the predicate result undefined
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescPtr,
        column_statistics: Vec<ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = vec![];
        for (i, s) in column_statistics.iter().enumerate() {
            let column = ColumnChunkMetaData::builder(schema_descr.column(i))
                .set_statistics(s.clone())
                .build()
                .unwrap();
            columns.push(column);
        }
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr(fields: Vec<(&str, PhysicalType)>) -> SchemaDescPtr {
        use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
        let mut schema_fields = fields
            .iter()
            .map(|(n, t)| {
                Arc::new(SchemaType::primitive_type_builder(n, *t).build().unwrap())
            })
            .collect::<Vec<_>>();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut schema_fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
