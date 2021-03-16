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

//! Distributed execution context.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryInto};
use std::{fs, time::Duration};

use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    execute_query_params::Query, job_status, ExecuteQueryParams, GetJobStatusParams,
    GetJobStatusResult,
};
use ballista_core::{
    client::BallistaClient,
    datasource::DFTableAdapter,
    error::{BallistaError, Result},
    memory_stream::MemoryStream,
};

use arrow::datatypes::Schema;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{DFSchema, Expr, LogicalPlan, Partitioning};
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::{dataframe::DataFrame, physical_plan::RecordBatchStream};
use log::{error, info};

#[allow(dead_code)]
struct BallistaContextState {
    /// Scheduler host
    scheduler_host: String,
    /// Scheduler port
    scheduler_port: u16,
    /// Tables that have been registered with this context
    tables: HashMap<String, LogicalPlan>,
    /// General purpose settings
    settings: HashMap<String, String>,
}

impl BallistaContextState {
    pub fn new(
        scheduler_host: String,
        scheduler_port: u16,
        settings: HashMap<String, String>,
    ) -> Self {
        Self {
            scheduler_host,
            scheduler_port,
            tables: HashMap::new(),
            settings,
        }
    }
}

#[allow(dead_code)]

pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
}

impl BallistaContext {
    /// Create a context for executing queries against a remote Ballista scheduler instance
    pub fn remote(host: &str, port: u16, settings: HashMap<String, String>) -> Self {
        let state = BallistaContextState::new(host.to_owned(), port, settings);

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Create a DataFrame representing a Parquet table scan

    pub fn read_parquet(&self, path: &str) -> Result<BallistaDataFrame> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        let df = ctx.read_parquet(path.to_str().unwrap())?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }

    /// Create a DataFrame representing a CSV table scan

    pub fn read_csv(&self, path: &str, options: CsvReadOptions) -> Result<BallistaDataFrame> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        let df = ctx.read_csv(path.to_str().unwrap(), options)?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(&self, name: &str, table: &BallistaDataFrame) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .tables
            .insert(name.to_owned(), table.to_logical_plan());
        Ok(())
    }

    pub fn register_csv(&self, name: &str, path: &str, options: CsvReadOptions) -> Result<()> {
        let df = self.read_csv(path, options)?;
        self.register_table(name, &df)
    }

    pub fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        let df = self.read_parquet(path)?;
        self.register_table(name, &df)
    }

    /// Create a DataFrame from a SQL statement
    pub fn sql(&self, sql: &str) -> Result<BallistaDataFrame> {
        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        // register tables
        let state = self.state.lock().unwrap();
        for (name, plan) in &state.tables {
            let plan = ctx.optimize(plan)?;
            let execution_plan = ctx.create_physical_plan(&plan)?;
            ctx.register_table(name, Arc::new(DFTableAdapter::new(plan, execution_plan)));
        }
        let df = ctx.sql(sql)?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }
}

/// The Ballista DataFrame is a wrapper around the DataFusion DataFrame and overrides the
/// `collect` method so that the query is executed against Ballista and not DataFusion.

pub struct BallistaDataFrame {
    /// Ballista context state
    state: Arc<Mutex<BallistaContextState>>,
    /// DataFusion DataFrame representing logical query plan
    df: Arc<dyn DataFrame>,
}

impl BallistaDataFrame {
    fn from(state: Arc<Mutex<BallistaContextState>>, df: Arc<dyn DataFrame>) -> Self {
        Self { state, df }
    }

    pub async fn collect(&self) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let scheduler_url = {
            let state = self.state.lock().unwrap();

            format!("http://{}:{}", state.scheduler_host, state.scheduler_port)
        };

        info!("Connecting to Ballista scheduler at {}", scheduler_url);

        let mut scheduler = SchedulerGrpcClient::connect(scheduler_url).await?;

        let plan = self.df.to_logical_plan();
        let schema: Schema = plan.schema().as_ref().clone().into();

        let job_id = scheduler
            .execute_query(ExecuteQueryParams {
                query: Some(Query::LogicalPlan((&plan).try_into()?)),
            })
            .await?
            .into_inner()
            .job_id;

        loop {
            let GetJobStatusResult { status } = scheduler
                .get_job_status(GetJobStatusParams {
                    job_id: job_id.clone(),
                })
                .await?
                .into_inner();
            let status = status.and_then(|s| s.status).ok_or_else(|| {
                BallistaError::Internal("Received empty status message".to_owned())
            })?;
            let wait_future = tokio::time::sleep(Duration::from_millis(100));
            match status {
                job_status::Status::Queued(_) => {
                    info!("Job {} still queued...", job_id);
                    wait_future.await;
                }
                job_status::Status::Running(_) => {
                    info!("Job {} is running...", job_id);
                    wait_future.await;
                }
                job_status::Status::Failed(err) => {
                    let msg = format!("Job {} failed: {}", job_id, err.error);
                    error!("{}", msg);
                    break Err(BallistaError::General(msg));
                }
                job_status::Status::Completed(completed) => {
                    // TODO: use streaming. Probably need to change the signature of fetch_partition to achieve that
                    let mut result = vec![];
                    for location in completed.partition_location {
                        let metadata = location.executor_meta.ok_or_else(|| {
                            BallistaError::Internal("Received empty executor metadata".to_owned())
                        })?;
                        let partition_id = location.partition_id.ok_or_else(|| {
                            BallistaError::Internal("Received empty partition id".to_owned())
                        })?;
                        let mut ballista_client =
                            BallistaClient::try_new(metadata.host.as_str(), metadata.port as u16)
                                .await?;
                        let stream = ballista_client
                            .fetch_partition(
                                &partition_id.job_id,
                                partition_id.stage_id as usize,
                                partition_id.partition_id as usize,
                            )
                            .await?;
                        result
                            .append(&mut datafusion::physical_plan::common::collect(stream).await?);
                    }
                    break Ok(Box::pin(MemoryStream::try_new(
                        result,
                        Arc::new(schema),
                        None,
                    )?));
                }
            };
        }
    }

    pub fn select_columns(&self, columns: &[&str]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .select_columns(columns)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn select(&self, expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.select(expr).map_err(BallistaError::from)?,
        ))
    }

    pub fn filter(&self, expr: Expr) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.filter(expr).map_err(BallistaError::from)?,
        ))
    }

    pub fn aggregate(&self, group_expr: &[Expr], aggr_expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .aggregate(group_expr, aggr_expr)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn limit(&self, n: usize) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.limit(n).map_err(BallistaError::from)?,
        ))
    }

    pub fn sort(&self, expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.sort(expr).map_err(BallistaError::from)?,
        ))
    }

    // TODO lifetime issue
    // pub fn join(&self, right: Arc<dyn DataFrame>, join_type: JoinType, left_cols: &[&str], right_cols: &[&str]) ->
    // Result<BallistaDataFrame> {     Ok(Self::from(self.state.clone(), self.df.join(right, join_type, &left_cols,
    // &right_cols).map_err(BallistaError::from)?)) }

    pub fn repartition(&self, partitioning_scheme: Partitioning) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .repartition(partitioning_scheme)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn schema(&self) -> &DFSchema {
        self.df.schema()
    }

    pub fn to_logical_plan(&self) -> LogicalPlan {
        self.df.to_logical_plan()
    }

    pub fn explain(&self, verbose: bool) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.explain(verbose).map_err(BallistaError::from)?,
        ))
    }
}

// #[async_trait]
// impl ExecutionContext for BallistaContext {
//     async fn get_executor_ids(&self) -> Result<Vec<ExecutorMeta>> {
//         match &self.config.discovery_mode {
//             DiscoveryMode::Etcd => etcd_get_executors(&self.config.etcd_urls, "default").await,
//             DiscoveryMode::Kubernetes => k8s_get_executors("default", "ballista").await,
//             DiscoveryMode::Standalone => Err(ballista_error("Standalone mode not implemented yet")),
//         }
//     }
//
//     async fn execute_task(
//         &self,
//         executor_meta: ExecutorMeta,
//         task: ExecutionTask,
//     ) -> Result<ShuffleId> {
//         // TODO what is the point of returning this info since it is based on input arg?
//         let shuffle_id = ShuffleId::new(task.job_uuid, task.stage_id, task.partition_id);
//
//         let _ = execute_action(
//             &executor_meta.host,
//             executor_meta.port,
//             &Action::Execute(task),
//         )
//         .await?;
//
//         Ok(shuffle_id)
//     }
//
//     async fn read_shuffle(&self, shuffle_id: &ShuffleId) -> Result<Vec<ColumnarBatch>> {
//         match self.shuffle_locations.get(shuffle_id) {
//             Some(executor_meta) => {
//                 let batches = execute_action(
//                     &executor_meta.host,
//                     executor_meta.port,
//                     &Action::FetchShuffle(*shuffle_id),
//                 )
//                 .await?;
//                 Ok(batches
//                     .iter()
//                     .map(|b| ColumnarBatch::from_arrow(b))
//                     .collect())
//             }
//             _ => Err(ballista_error(&format!(
//                 "Failed to resolve executor UUID for shuffle ID {:?}",
//                 shuffle_id
//             ))),
//         }
//     }
//
//     fn config(&self) -> ExecutorConfig {
//         self.config.clone()
//     }
// }
