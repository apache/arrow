// Copyright 2021 Andy Grove
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

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{fs::File, pin::Pin};

use crate::error::{BallistaError, Result};
use crate::execution_plans::{QueryStageExec, UnresolvedShuffleExec};
use crate::memory_stream::MemoryStream;
use crate::serde::scheduler::PartitionStats;
use arrow::array::{
    ArrayBuilder, ArrayRef, StructArray, StructBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::csv::CsvExec;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, RecordBatchStream};
use futures::StreamExt;

/// Stream data to disk in Arrow IPC format

pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send + Sync>>,
    path: &str,
) -> Result<PartitionStats> {
    let file = File::create(&path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to create partition file at {}: {:?}",
            path, e
        ))
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;
    let mut writer = FileWriter::try_new(file, stream.schema().as_ref())?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send + Sync>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn format_plan(plan: &dyn ExecutionPlan, indent: usize) -> Result<String> {
    let operator_str = if let Some(exec) = plan.as_any().downcast_ref::<HashAggregateExec>() {
        format!(
            "HashAggregateExec: groupBy={:?}, aggrExpr={:?}",
            exec.group_expr()
                .iter()
                .map(|e| format_expr(e.0.as_ref()))
                .collect::<Vec<String>>(),
            exec.aggr_expr()
                .iter()
                .map(|e| format_agg_expr(e.as_ref()))
                .collect::<Result<Vec<String>>>()?
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
        format!(
            "HashJoinExec: joinType={:?}, on={:?}",
            exec.join_type(),
            exec.on()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        let mut num_files = 0;
        for part in exec.partitions() {
            num_files += part.filenames().len();
        }
        format!(
            "ParquetExec: partitions={}, files={}",
            exec.partitions().len(),
            num_files
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<CsvExec>() {
        format!(
            "CsvExec: {}; partitions={}",
            &exec.path(),
            exec.output_partitioning().partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        format!("FilterExec: {}", format_expr(exec.predicate().as_ref()))
    } else if let Some(exec) = plan.as_any().downcast_ref::<QueryStageExec>() {
        format!(
            "QueryStageExec: job={}, stage={}",
            exec.job_id, exec.stage_id
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        format!("UnresolvedShuffleExec: stages={:?}", exec.query_stage_ids)
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        format!(
            "CoalesceBatchesExec: batchSize={}",
            exec.target_batch_size()
        )
    } else if plan.as_any().downcast_ref::<MergeExec>().is_some() {
        "MergeExec".to_string()
    } else {
        let str = format!("{:?}", plan);
        String::from(&str[0..120])
    };

    let children_str = plan
        .children()
        .iter()
        .map(|c| format_plan(c.as_ref(), indent + 1))
        .collect::<Result<Vec<String>>>()?
        .join("\n");

    let indent_str = "  ".repeat(indent);
    if plan.children().is_empty() {
        Ok(format!("{}{}{}", indent_str, &operator_str, children_str))
    } else {
        Ok(format!("{}{}\n{}", indent_str, &operator_str, children_str))
    }
}

pub fn format_agg_expr(expr: &dyn AggregateExpr) -> Result<String> {
    Ok(format!(
        "{} {:?}",
        expr.field()?.name(),
        expr.expressions()
            .iter()
            .map(|e| format_expr(e.as_ref()))
            .collect::<Vec<String>>()
    ))
}

pub fn format_expr(expr: &dyn PhysicalExpr) -> String {
    if let Some(e) = expr.as_any().downcast_ref::<Column>() {
        e.name().to_string()
    } else if let Some(e) = expr.as_any().downcast_ref::<Literal>() {
        e.to_string()
    } else if let Some(e) = expr.as_any().downcast_ref::<BinaryExpr>() {
        format!("{} {} {}", e.left(), e.op(), e.right())
    } else {
        format!("{}", expr)
    }
}

pub fn produce_diagram(filename: &str, stages: &[Arc<QueryStageExec>]) -> Result<()> {
    let write_file = File::create(filename)?;
    let mut w = BufWriter::new(&write_file);
    writeln!(w, "digraph G {{")?;

    // draw stages and entities
    for stage in stages {
        writeln!(w, "\tsubgraph cluster{} {{", stage.stage_id)?;
        writeln!(w, "\t\tlabel = \"Stage {}\";", stage.stage_id)?;
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(&mut w, stage.child.as_ref(), stage.stage_id, &mut id, true)?;
        writeln!(w, "\t}}")?;
    }

    // draw relationships
    for stage in stages {
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(&mut w, stage.child.as_ref(), stage.stage_id, &mut id, false)?;
    }

    write!(w, "}}")?;
    Ok(())
}

fn build_exec_plan_diagram(
    w: &mut BufWriter<&File>,
    plan: &dyn ExecutionPlan,
    stage_id: usize,
    id: &mut AtomicUsize,
    draw_entity: bool,
) -> Result<usize> {
    let operator_str = if plan.as_any().downcast_ref::<HashAggregateExec>().is_some() {
        "HashAggregateExec"
    } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
        "SortExec"
    } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
        "ProjectionExec"
    } else if plan.as_any().downcast_ref::<HashJoinExec>().is_some() {
        "HashJoinExec"
    } else if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
        "ParquetExec"
    } else if plan.as_any().downcast_ref::<CsvExec>().is_some() {
        "CsvExec"
    } else if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        "FilterExec"
    } else if plan.as_any().downcast_ref::<QueryStageExec>().is_some() {
        "QueryStageExec"
    } else if plan
        .as_any()
        .downcast_ref::<UnresolvedShuffleExec>()
        .is_some()
    {
        "UnresolvedShuffleExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
    {
        "CoalesceBatchesExec"
    } else if plan.as_any().downcast_ref::<MergeExec>().is_some() {
        "MergeExec"
    } else {
        println!("Unknown: {:?}", plan);
        "Unknown"
    };

    let node_id = id.load(Ordering::SeqCst);
    id.store(node_id + 1, Ordering::SeqCst);

    if draw_entity {
        writeln!(
            w,
            "\t\tstage_{}_exec_{} [shape=box, label=\"{}\"];",
            stage_id, node_id, operator_str
        )?;
    }
    for child in plan.children() {
        if let Some(shuffle) = child.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            if !draw_entity {
                for y in &shuffle.query_stage_ids {
                    writeln!(
                        w,
                        "\tstage_{}_exec_1 -> stage_{}_exec_{};",
                        y, stage_id, node_id
                    )?;
                }
            }
        } else {
            // relationships within same entity
            let child_id = build_exec_plan_diagram(w, child.as_ref(), stage_id, id, draw_entity)?;
            if draw_entity {
                writeln!(
                    w,
                    "\t\tstage_{}_exec_{} -> stage_{}_exec_{};",
                    stage_id, child_id, stage_id, node_id
                )?;
            }
        }
    }
    Ok(node_id)
}
