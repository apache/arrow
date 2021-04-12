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

pub(crate) async fn list_executors_data(
    data_server: SchedulerServer,
) -> Result<impl warp::Reply, Rejection> {
    let data: Result<Response<GetExecutorMetadataResult>, tonic::Status> = data_server
        .get_executors_metadata(Request::new(GetExecutorMetadataParams {}))
        .await;
    let result = data.unwrap();
    let res: &GetExecutorMetadataResult = result.get_ref();
    let vec: &Vec<ExecutorMetadata> = &res.metadata;
    let metadata: Vec<ExecutorMeta> = vec
        .iter()
        .map(|v: &ExecutorMetadata| ExecutorMeta {
            host: v.host.clone(),
            port: v.port as u16,
            id: v.id.clone(),
        })
        .collect();
    Ok(warp::reply::json(&metadata))
}
