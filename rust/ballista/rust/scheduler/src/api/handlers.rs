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

use crate::SchedulerServer;
use ballista_core::serde::protobuf::{
    scheduler_grpc_server::SchedulerGrpc, ExecutorMetadata, GetExecutorMetadataParams,
    GetExecutorMetadataResult,
};
use ballista_core::serde::scheduler::ExecutorMeta;
use tonic::{Request, Response};
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct StateResponse {
    executors: Vec<ExecutorMeta>,
    started: u128,
    version: String,
}

pub(crate) async fn scheduler_state(
    data_server: SchedulerServer,
) -> Result<impl warp::Reply, Rejection> {
    let data: Result<Response<GetExecutorMetadataResult>, tonic::Status> = data_server
        .get_executors_metadata(Request::new(GetExecutorMetadataParams {}))
        .await;
    let metadata: Vec<ExecutorMeta> = match data {
        Ok(result) => {
            let res: &GetExecutorMetadataResult = result.get_ref();
            let vec: &Vec<ExecutorMetadata> = &res.metadata;
            vec.iter()
                .map(|v: &ExecutorMetadata| ExecutorMeta {
                    host: v.host.clone(),
                    port: v.port as u16,
                    id: v.id.clone(),
                })
                .collect()
        }
        Err(_) => vec![],
    };
    let response = StateResponse {
        executors: metadata,
        started: data_server.start_time,
        version: data_server.version.clone(),
    };
    Ok(warp::reply::json(&response))
}
