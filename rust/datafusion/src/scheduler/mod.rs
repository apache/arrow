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

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::arrow::record_batch::RecordBatch;
use crate::error::ExecutionError;
use crate::error::Result;
use crate::physical_plan::shuffle::{ShuffleExchangeExec, ShuffleReaderExec};
use crate::physical_plan::ExecutionPlan;

use crate::execution::context::ExecutionContext;
use uuid::Uuid;

/// A Job typically represents a single query and the query is executed in stages. Stages are
/// separated by map operations (shuffles) to re-partition data before the next stage starts.
#[derive(Debug)]
pub struct Job {
    /// Job UUID
    pub id: Uuid,
    /// A list of stages within this job. There can be dependencies between stages to form
    /// a directed acyclic graph (DAG).
    pub stages: Vec<Rc<RefCell<Stage>>>,
    /// The root stage id that produces the final results
    pub root_stage_id: usize,
}

impl Job {
    pub fn explain(&self) {
        println!("Job {} has {} stages:\n", self.id, self.stages.len());
        self.stages.iter().for_each(|stage| {
            let stage = stage.as_ref().borrow();
            println!("Stage {}:\n", stage.id);
            if stage.prior_stages.is_empty() {
                println!("Stage {} has no dependencies.", stage.id);
            } else {
                println!(
                    "Stage {} depends on stages {:?}.",
                    stage.id, stage.prior_stages
                );
            }
            println!(
                "\n{:?}\n",
                stage
                    .plan
                    .as_ref()
                    .expect("Stages should always have a plan")
            );
        })
    }
}

/// A query stage represents a portion of a physical plan with the same partitioning
/// scheme throughout, meaning that each partition can be executed in parallel. Query
/// stages form a DAG.
#[derive(Debug)]
pub struct Stage {
    /// Stage id which is unique within a job.
    pub id: usize,
    /// A list of stages that must complete before this stage can execute.
    pub prior_stages: Vec<usize>,
    /// The physical plan to execute for this stage
    pub plan: Option<Arc<dyn ExecutionPlan>>,
}

impl Stage {
    /// Create a new empty stage with the specified id.
    fn new(id: usize) -> Self {
        Self {
            id,
            prior_stages: vec![],
            plan: None,
        }
    }
}

/// Task that can be sent to an executor for execution. Tasks represent single partitions
/// within stagees.
#[derive(Debug, Clone)]
pub struct ExecutionTask {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
}

impl ExecutionTask {
    pub fn new(
        job_uuid: Uuid,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    ) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
            plan,
            shuffle_locations,
        }
    }

    pub fn key(&self) -> String {
        format!("{}.{}.{}", self.job_uuid, self.stage_id, self.partition_id)
    }
}

/// Unique identifier for the output shuffle partition of an operator.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShuffleId {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
}

impl ShuffleId {
    pub fn new(job_uuid: Uuid, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
        }
    }
}

/// Create a Job (DAG of stages) from a physical execution plan.
pub fn create_job(plan: Arc<dyn ExecutionPlan>) -> Result<Job> {
    let mut scheduler = JobScheduler::new();
    scheduler.create_job(plan)?;
    Ok(scheduler.job)
}

pub struct JobScheduler {
    job: Job,
    next_stage_id: usize,
}

impl JobScheduler {
    fn new() -> Self {
        let job = Job {
            id: Uuid::new_v4(),
            stages: vec![],
            root_stage_id: 0,
        };
        Self {
            job,
            next_stage_id: 0,
        }
    }

    fn create_job(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let new_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let new_stage = Rc::new(RefCell::new(Stage::new(new_stage_id)));
        self.job.stages.push(new_stage.clone());
        let plan = self.visit_plan(plan, new_stage.clone())?;
        new_stage.as_ref().borrow_mut().plan = Some(plan);
        Ok(())
    }

    fn visit_plan(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        current_stage: Rc<RefCell<Stage>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(exchange) =
            plan.as_ref().as_any().downcast_ref::<ShuffleExchangeExec>()
        {
            // shuffle indicates that we need a new stage
            let new_stage_id = self.next_stage_id;
            self.next_stage_id += 1;
            let new_stage = Rc::new(RefCell::new(Stage::new(new_stage_id)));
            self.job.stages.push(new_stage.clone());

            // the children need to be part of this new stage
            let shuffle_input =
                self.visit_plan(exchange.child.clone(), new_stage.clone())?;

            new_stage.as_ref().borrow_mut().plan = Some(shuffle_input);

            // the current stage depends on this new stage
            current_stage
                .as_ref()
                .borrow_mut()
                .prior_stages
                .push(new_stage_id);

            // return a shuffle reader to read the results from the stage
            let n = exchange.child.output_partitioning().partition_count();

            let shuffle_id = (0..n)
                .map(|n| ShuffleId {
                    job_uuid: self.job.id,
                    stage_id: new_stage_id,
                    partition_id: n,
                })
                .collect();
            Ok(Arc::new(ShuffleReaderExec::new(
                exchange.schema(),
                shuffle_id,
            )))
        } else {
            let new_children = plan
                .children()
                .iter()
                .map(|child| self.visit_plan(child.clone(), current_stage.clone()))
                .collect::<Result<Vec<_>>>()?;
            plan.with_new_children(new_children)
        }
    }
}

enum StageStatus {
    Pending,
    Completed,
}

enum TaskStatus {
    Pending(Instant),
    Running(Instant),
    Completed(ShuffleId),
    Failed(String),
}

#[derive(Debug, Clone)]
struct ExecutorShuffleIds {
    executor_id: String,
    shuffle_ids: Vec<ShuffleId>,
}

/// Execute a job directly against executors as starting point
pub async fn execute_job(job: &Job, ctx: &ExecutionContext) -> Result<Vec<RecordBatch>> {
    let executors: Vec<ExecutorMeta> = vec![]; //ctx.get_executor_ids().await?;

    println!("Executors: {:?}", executors);

    if executors.is_empty() {
        println!("no executors found");
        return Err(ExecutionError::General(
            "no executors available".to_string(),
        ));
    }

    let mut shuffle_location_map: HashMap<ShuffleId, ExecutorMeta> = HashMap::new();

    let mut stage_status_map = HashMap::new();

    for stage in &job.stages {
        let stage = stage.borrow_mut();
        stage_status_map.insert(stage.id, StageStatus::Pending);
    }

    // loop until all stages are complete
    let mut num_completed = 0;
    while num_completed < job.stages.len() {
        num_completed = 0;

        //TODO do stages in parallel when possible
        for stage in &job.stages {
            let stage = stage.borrow_mut();
            let status = stage_status_map.get(&stage.id).unwrap();
            match status {
                StageStatus::Pending => {
                    // have prior stages already completed ?
                    if stage.prior_stages.iter().all(|id| {
                        match stage_status_map.get(id) {
                            Some(StageStatus::Completed) => true,
                            _ => false,
                        }
                    }) {
                        println!("Running stage {}", stage.id);
                        let plan = stage
                            .plan
                            .as_ref()
                            .expect("all stages should have plans at execution time");

                        let stage_start = Instant::now();

                        let exec = plan;
                        let parts = exec.output_partitioning().partition_count();

                        // build queue of tasks per executor
                        let mut next_executor_id = 0;
                        let mut executor_tasks: HashMap<String, Vec<ExecutionTask>> =
                            HashMap::new();
                        #[allow(clippy::needless_range_loop)]
                        for i in 0..executors.len() {
                            //executor_tasks.insert(executors[i].id.clone(), vec![]);
                        }
                        for partition in 0..parts {
                            let task = ExecutionTask::new(
                                job.id,
                                stage.id,
                                partition,
                                plan.clone(),
                                shuffle_location_map.clone(),
                            );

                            // load balance across the executors
                            let executor_meta = &executors[next_executor_id];
                            next_executor_id += 1;
                            if next_executor_id == executors.len() {
                                next_executor_id = 0;
                            }

                            let queue = executor_tasks
                                .get_mut(&executor_meta.id)
                                .expect("executor queue should exist");

                            queue.push(task);
                        }

                        //TODO execution

                        let mut stage_shuffle_ids: Vec<ExecutorShuffleIds> = vec![];
                        // for thread in threads {
                        //     stage_shuffle_ids.push(thread.join().unwrap()?);
                        // }
                        println!(
                            "Stage {} completed in {} ms and produced {} shuffles",
                            stage.id,
                            stage_start.elapsed().as_millis(),
                            stage_shuffle_ids.len()
                        );

                        for executor_shuffle_ids in &stage_shuffle_ids {
                            for shuffle_id in &executor_shuffle_ids.shuffle_ids {
                                let executor = executors
                                    .iter()
                                    .find(|e| e.id == executor_shuffle_ids.executor_id)
                                    .unwrap();
                                shuffle_location_map
                                    .insert(*shuffle_id, executor.clone());
                            }
                        }
                        stage_status_map.insert(stage.id, StageStatus::Completed);

                        if stage.id == job.root_stage_id {
                            // TODO end of query
                        }
                    } else {
                        println!("Cannot run stage {} yet", stage.id);
                    }
                }
                StageStatus::Completed => {
                    num_completed += 1;
                }
            }
        }
    }

    unreachable!()
}

/// Executor represents a thread
#[derive(Debug, Clone)]
pub struct ExecutorMeta {
    pub id: String,
}
