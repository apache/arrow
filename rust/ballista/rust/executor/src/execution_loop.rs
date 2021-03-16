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

use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::{sync::Arc, time::Duration};

use datafusion::physical_plan::ExecutionPlan;
use log::{debug, error, info, warn};
use tonic::transport::Channel;

use ballista_core::serde::scheduler::ExecutorMeta;
use ballista_core::{
    client::BallistaClient,
    serde::protobuf::{
        self, scheduler_grpc_client::SchedulerGrpcClient, task_status, FailedTask, PartitionId,
        PollWorkParams, PollWorkResult, TaskDefinition, TaskStatus,
    },
};
use protobuf::CompletedTask;

pub async fn poll_loop(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor_client: BallistaClient,
    executor_meta: ExecutorMeta,
    concurrent_tasks: usize,
) {
    let executor_meta: protobuf::ExecutorMetadata = executor_meta.into();
    let available_tasks_slots = Arc::new(AtomicUsize::new(concurrent_tasks));
    let (task_status_sender, mut task_status_receiver) = std::sync::mpsc::channel::<TaskStatus>();

    loop {
        debug!("Starting registration loop with scheduler");

        let task_status: Vec<TaskStatus> = sample_tasks_status(&mut task_status_receiver).await;

        let poll_work_result: anyhow::Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor_meta.clone()),
                    can_accept_task: available_tasks_slots.load(Ordering::SeqCst) > 0,
                    task_status,
                })
                .await;

        let task_status_sender = task_status_sender.clone();

        match poll_work_result {
            Ok(result) => {
                if let Some(task) = result.into_inner().task {
                    run_received_tasks(
                        executor_client.clone(),
                        executor_meta.id.clone(),
                        available_tasks_slots.clone(),
                        task_status_sender,
                        task,
                    )
                    .await;
                }
            }
            Err(error) => {
                warn!("Executor registration failed. If this continues to happen the executor might be marked as dead by the scheduler. Error: {}", error);
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn run_received_tasks(
    mut executor_client: BallistaClient,
    executor_id: String,
    available_tasks_slots: Arc<AtomicUsize>,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
) {
    info!("Received task {:?}", task.task_id.as_ref().unwrap());
    available_tasks_slots.fetch_sub(1, Ordering::SeqCst);
    let plan: Arc<dyn ExecutionPlan> = (&task.plan.unwrap()).try_into().unwrap();
    let task_id = task.task_id.unwrap();
    // TODO: This is a convoluted way of executing the task. We should move the task
    // execution code outside of the FlightService (data plane) into the control plane.

    tokio::spawn(async move {
        let execution_result = executor_client
            .execute_partition(
                task_id.job_id.clone(),
                task_id.stage_id as usize,
                vec![task_id.partition_id as usize],
                plan,
            )
            .await;
        info!("DONE WITH TASK: {:?}", execution_result);
        available_tasks_slots.fetch_add(1, Ordering::SeqCst);
        let _ = task_status_sender.send(as_task_status(
            execution_result.map(|_| ()),
            executor_id,
            task_id,
        ));
    });
}

fn as_task_status(
    execution_result: ballista_core::error::Result<()>,
    executor_id: String,
    task_id: PartitionId,
) -> TaskStatus {
    match execution_result {
        Ok(_) => {
            info!("Task {:?} finished", task_id);

            TaskStatus {
                partition_id: Some(task_id),
                status: Some(task_status::Status::Completed(CompletedTask {
                    executor_id,
                })),
            }
        }
        Err(e) => {
            let error_msg = e.to_string();
            info!("Task {:?} failed: {}", task_id, error_msg);

            TaskStatus {
                partition_id: Some(task_id),
                status: Some(task_status::Status::Failed(FailedTask {
                    error: format!("Task failed due to Tokio error: {}", error_msg),
                })),
            }
        }
    }
}

async fn sample_tasks_status(task_status_receiver: &mut Receiver<TaskStatus>) -> Vec<TaskStatus> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        match task_status_receiver.try_recv() {
            anyhow::Result::Ok(status) => {
                task_status.push(status);
            }
            Err(TryRecvError::Empty) => {
                break;
            }
            Err(TryRecvError::Disconnected) => {
                error!("Task statuses channel disconnected");
            }
        }
    }

    task_status
}
