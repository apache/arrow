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

//! Distributed query execution
//!
//! This code is EXPERIMENTAL and still under development

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, future::Future};

use ballista_core::client::BallistaClient;
use ballista_core::datasource::DFTableAdapter;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::scheduler::ExecutorMeta;
use ballista_core::serde::scheduler::PartitionId;
use ballista_core::utils::format_plan;
use ballista_core::{
    execution_plans::{QueryStageExec, ShuffleReaderExec, UnresolvedShuffleExec},
    serde::scheduler::PartitionLocation,
};
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, info};
use tokio::task::JoinHandle;

type SendableExecutionPlan = Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send>>;
type PartialQueryStageResult = (Arc<dyn ExecutionPlan>, Vec<Arc<QueryStageExec>>);

pub struct DistributedPlanner {
    executors: Vec<ExecutorMeta>,
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn try_new(executors: Vec<ExecutorMeta>) -> Result<Self> {
        if executors.is_empty() {
            Err(BallistaError::General(
                "DistributedPlanner requires at least one executor".to_owned(),
            ))
        } else {
            Ok(Self {
                executors,
                next_stage_id: 0,
            })
        }
    }
}

impl DistributedPlanner {
    /// Execute a distributed query against a cluster, leaving the final results on the
    /// executors. The [ExecutionPlan] returned by this method is guaranteed to be a
    /// [ShuffleReaderExec] that can be used to fetch the final results from the executors
    /// in parallel.
    pub async fn execute_distributed_query(
        &mut self,
        job_id: String,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let now = Instant::now();
        let execution_plans = self.plan_query_stages(&job_id, execution_plan)?;

        info!(
            "DistributedPlanner created {} execution plans in {} seconds:",
            execution_plans.len(),
            now.elapsed().as_secs()
        );

        for plan in &execution_plans {
            info!("{}", format_plan(plan.as_ref(), 0)?);
        }

        execute(execution_plans, self.executors.clone()).await
    }

    /// Returns a vector of ExecutionPlans, where the root node is a [QueryStageExec].
    /// Plans that depend on the input of other plans will have leaf nodes of type [UnresolvedShuffleExec].
    /// A [QueryStageExec] is created whenever the partitioning changes.
    ///
    /// Returns an empty vector if the execution_plan doesn't need to be sliced into several stages.
    pub fn plan_query_stages(
        &mut self,
        job_id: &str,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<QueryStageExec>>> {
        info!("planning query stages");
        let (new_plan, mut stages) = self.plan_query_stages_internal(job_id, execution_plan)?;
        stages.push(create_query_stage(
            job_id.to_string(),
            self.next_stage_id(),
            new_plan,
        )?);
        Ok(stages)
    }

    /// Returns a potentially modified version of the input execution_plan along with the resulting query stages.
    /// This function is needed because the input execution_plan might need to be modified, but it might not hold a
    /// compelte query stage (its parent might also belong to the same stage)
    fn plan_query_stages_internal(
        &mut self,
        job_id: &str,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<PartialQueryStageResult> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok((execution_plan, vec![]));
        }

        let mut stages = vec![];
        let mut children = vec![];
        for child in execution_plan.children() {
            let (new_child, mut child_stages) =
                self.plan_query_stages_internal(job_id, child.clone())?;
            children.push(new_child);
            stages.append(&mut child_stages);
        }

        if let Some(adapter) = execution_plan.as_any().downcast_ref::<DFTableAdapter>() {
            let ctx = ExecutionContext::new();
            Ok((ctx.create_physical_plan(&adapter.logical_plan)?, stages))
        } else if let Some(merge) = execution_plan.as_any().downcast_ref::<MergeExec>() {
            let query_stage = create_query_stage(
                job_id.to_string(),
                self.next_stage_id(),
                merge.children()[0].clone(),
            )?;
            let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                vec![query_stage.stage_id],
                query_stage.schema(),
                query_stage.output_partitioning().partition_count(),
            ));
            stages.push(query_stage);
            Ok((merge.with_new_children(vec![unresolved_shuffle])?, stages))
        } else if let Some(agg) = execution_plan.as_any().downcast_ref::<HashAggregateExec>() {
            //TODO should insert query stages in more generic way based on partitioning metadata
            // and not specifically for this operator
            match agg.mode() {
                AggregateMode::Final => {
                    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
                    for child in &children {
                        let new_stage = create_query_stage(
                            job_id.to_string(),
                            self.next_stage_id(),
                            child.clone(),
                        )?;
                        new_children.push(Arc::new(UnresolvedShuffleExec::new(
                            vec![new_stage.stage_id],
                            new_stage.schema().clone(),
                            new_stage.output_partitioning().partition_count(),
                        )));
                        stages.push(new_stage);
                    }
                    Ok((agg.with_new_children(new_children)?, stages))
                }
                AggregateMode::Partial => Ok((agg.with_new_children(children)?, stages)),
            }
        } else if let Some(join) = execution_plan.as_any().downcast_ref::<HashJoinExec>() {
            Ok((join.with_new_children(children)?, stages))
        } else {
            // TODO check for compatible partitioning schema, not just count
            if execution_plan.output_partitioning().partition_count()
                != children[0].output_partitioning().partition_count()
            {
                let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for child in &children {
                    let new_stage = create_query_stage(
                        job_id.to_string(),
                        self.next_stage_id(),
                        child.clone(),
                    )?;
                    new_children.push(Arc::new(UnresolvedShuffleExec::new(
                        vec![new_stage.stage_id],
                        new_stage.schema().clone(),
                        new_stage.output_partitioning().partition_count(),
                    )));
                    stages.push(new_stage);
                }
                Ok((execution_plan.with_new_children(new_children)?, stages))
            } else {
                Ok((execution_plan.with_new_children(children)?, stages))
            }
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

fn execute(
    stages: Vec<Arc<QueryStageExec>>,
    executors: Vec<ExecutorMeta>,
) -> SendableExecutionPlan {
    Box::pin(async move {
        let mut partition_locations: HashMap<usize, Vec<PartitionLocation>> = HashMap::new();
        let mut result_partition_locations = vec![];
        for stage in &stages {
            debug!("execute() {}", &format!("{:?}", stage)[0..60]);
            let stage = remove_unresolved_shuffles(stage.as_ref(), &partition_locations)?;
            let stage = stage.as_any().downcast_ref::<QueryStageExec>().unwrap();
            result_partition_locations = execute_query_stage(
                &stage.job_id.clone(),
                stage.stage_id,
                stage.children()[0].clone(),
                executors.clone(),
            )
            .await?;
            partition_locations.insert(stage.stage_id, result_partition_locations.clone());
        }

        let shuffle_reader: Arc<dyn ExecutionPlan> = Arc::new(ShuffleReaderExec::try_new(
            result_partition_locations,
            stages.last().unwrap().schema(),
        )?);
        Ok(shuffle_reader)
    })
}

pub fn remove_unresolved_shuffles(
    stage: &dyn ExecutionPlan,
    partition_locations: &HashMap<usize, Vec<PartitionLocation>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(unresolved_shuffle) = child.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            let mut relevant_locations = vec![];
            for id in &unresolved_shuffle.query_stage_ids {
                relevant_locations.append(
                    &mut partition_locations
                        .get(id)
                        .ok_or_else(|| {
                            BallistaError::General(
                                "Missing partition location. Could not remove unresolved shuffles"
                                    .to_owned(),
                            )
                        })?
                        .clone(),
                );
            }
            new_children.push(Arc::new(ShuffleReaderExec::try_new(
                relevant_locations,
                unresolved_shuffle.schema().clone(),
            )?))
        } else {
            new_children.push(remove_unresolved_shuffles(
                child.as_ref(),
                partition_locations,
            )?);
        }
    }
    Ok(stage.with_new_children(new_children)?)
}

fn create_query_stage(
    job_id: String,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<QueryStageExec>> {
    Ok(Arc::new(QueryStageExec::try_new(job_id, stage_id, plan)?))
}

/// Execute a query stage by sending each partition to an executor
async fn execute_query_stage(
    job_id: &str,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Result<Vec<PartitionLocation>> {
    info!(
        "execute_query_stage() stage_id={}\n{}",
        stage_id,
        format_plan(plan.as_ref(), 0)?
    );

    let partition_count = plan.output_partitioning().partition_count();

    let num_chunks = partition_count / executors.len();
    let num_chunks = num_chunks.max(1);
    let partition_chunks: Vec<Vec<usize>> = (0..partition_count)
        .collect::<Vec<usize>>()
        .chunks(num_chunks)
        .map(|r| r.to_vec())
        .collect();

    info!(
        "Executing query stage with {} chunks of partition ranges",
        partition_chunks.len()
    );

    let mut executions: Vec<JoinHandle<Result<Vec<PartitionLocation>>>> =
        Vec::with_capacity(partition_count);
    for i in 0..partition_chunks.len() {
        let plan = plan.clone();
        let executor_meta = executors[i % executors.len()].clone();
        let partition_ids = partition_chunks[i].to_vec();
        let job_id = job_id.to_owned();
        executions.push(tokio::spawn(async move {
            let mut client =
                BallistaClient::try_new(&executor_meta.host, executor_meta.port).await?;
            let stats = client
                .execute_partition(job_id.clone(), stage_id, partition_ids.clone(), plan)
                .await?;

            Ok(partition_ids
                .iter()
                .map(|part| PartitionLocation {
                    partition_id: PartitionId::new(&job_id, stage_id, *part),
                    executor_meta: executor_meta.clone(),
                    partition_stats: *stats[*part].statistics(),
                })
                .collect())
        }));
    }

    // wait for all partitions to complete
    let results = futures::future::join_all(executions).await;

    // check for errors
    let mut meta = Vec::with_capacity(partition_count);
    for result in results {
        match result {
            Ok(partition_result) => {
                let final_result = partition_result?;
                debug!("Query stage partition result: {:?}", final_result);
                meta.extend(final_result);
            }
            Err(e) => {
                return Err(BallistaError::General(format!(
                    "Query stage {} failed: {:?}",
                    stage_id, e
                )))
            }
        }
    }

    debug!(
        "execute_query_stage() stage_id={} produced {:?}",
        stage_id, meta
    );

    Ok(meta)
}

#[cfg(test)]
mod test {
    use crate::planner::DistributedPlanner;
    use crate::test_utils::datafusion_test_context;
    use ballista_core::error::BallistaError;
    use ballista_core::execution_plans::UnresolvedShuffleExec;
    use ballista_core::serde::protobuf;
    use ballista_core::serde::scheduler::ExecutorMeta;
    use ballista_core::utils::format_plan;
    use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
    use datafusion::physical_plan::merge::MergeExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sort::SortExec;
    use datafusion::physical_plan::ExecutionPlan;
    use std::convert::TryInto;
    use std::sync::Arc;
    use uuid::Uuid;

    macro_rules! downcast_exec {
        ($exec: expr, $ty: ty) => {
            $exec.as_any().downcast_ref::<$ty>().unwrap()
        };
    }

    #[test]
    fn test() -> Result<(), BallistaError> {
        let mut ctx = datafusion_test_context("testdata")?;

        // simplified form of TPC-H query 1
        let df = ctx.sql(
            "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
            from lineitem
            group by l_returnflag
            order by l_returnflag",
        )?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        let mut planner = DistributedPlanner::try_new(vec![ExecutorMeta {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
        }])?;
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(&job_uuid.to_string(), plan)?;
        for stage in &stages {
            println!("{}", format_plan(stage.as_ref(), 0)?);
        }

        /* Expected result:
        QueryStageExec: job=f011432e-e424-4016-915d-e3d8b84f6dbd, stage=1
         HashAggregateExec: groupBy=["l_returnflag"], aggrExpr=["SUM(l_extendedprice Multiply Int64(1)) [\"l_extendedprice * CAST(1 AS Float64)\"]"]
          CsvExec: testdata/lineitem; partitions=2

        QueryStageExec: job=f011432e-e424-4016-915d-e3d8b84f6dbd, stage=2
         MergeExec
          UnresolvedShuffleExec: stages=[1]

        QueryStageExec: job=f011432e-e424-4016-915d-e3d8b84f6dbd, stage=3
         SortExec { input: ProjectionExec { expr: [(Column { name: "l_returnflag" }, "l_returnflag"), (Column { name: "SUM(l_ext
          ProjectionExec { expr: [(Column { name: "l_returnflag" }, "l_returnflag"), (Column { name: "SUM(l_extendedprice Multip
           HashAggregateExec: groupBy=["l_returnflag"], aggrExpr=["SUM(l_extendedprice Multiply Int64(1)) [\"l_extendedprice * CAST(1 AS Float64)\"]"]
            UnresolvedShuffleExec: stages=[2]
        */

        let sort = stages[2].children()[0].clone();
        let sort = downcast_exec!(sort, SortExec);

        let projection = sort.children()[0].clone();
        println!("{:?}", projection);
        let projection = downcast_exec!(projection, ProjectionExec);

        let final_hash = projection.children()[0].clone();
        let final_hash = downcast_exec!(final_hash, HashAggregateExec);

        let unresolved_shuffle = final_hash.children()[0].clone();
        let unresolved_shuffle = downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.query_stage_ids, vec![2]);

        let merge_exec = stages[1].children()[0].clone();
        let merge_exec = downcast_exec!(merge_exec, MergeExec);

        let unresolved_shuffle = merge_exec.children()[0].clone();
        let unresolved_shuffle = downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.query_stage_ids, vec![1]);

        let partial_hash = stages[0].children()[0].clone();
        let partial_hash_serde = roundtrip_operator(partial_hash.clone())?;

        let partial_hash = downcast_exec!(partial_hash, HashAggregateExec);
        let partial_hash_serde = downcast_exec!(partial_hash_serde, HashAggregateExec);

        assert_eq!(
            format!("{:?}", partial_hash),
            format!("{:?}", partial_hash_serde)
        );

        Ok(())
    }

    fn roundtrip_operator(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        let proto: protobuf::PhysicalPlanNode = plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        Ok(result_exec_plan)
    }
}
