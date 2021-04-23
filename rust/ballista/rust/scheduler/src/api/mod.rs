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

mod handlers;

use crate::SchedulerServer;
use anyhow::Result;
use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll},
};
use warp::filters::BoxedFilter;
use warp::{Buf, Filter, Reply};

pub enum EitherBody<A, B> {
    Left(A),
    Right(B),
}

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type HttpBody = dyn http_body::Body<Data = dyn Buf, Error = Error> + 'static;

impl<A, B> http_body::Body for EitherBody<A, B>
where
    A: http_body::Body + Send + Unpin,
    B: http_body::Body<Data = A::Data> + Send + Unpin,
    A::Error: Into<Error>,
    B::Error: Into<Error>,
{
    type Data = A::Data;
    type Error = Error;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        match self.get_mut() {
            EitherBody::Left(b) => Pin::new(b).poll_data(cx).map(map_option_err),
            EitherBody::Right(b) => Pin::new(b).poll_data(cx).map(map_option_err),
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        match self.get_mut() {
            EitherBody::Left(b) => Pin::new(b).poll_trailers(cx).map_err(Into::into),
            EitherBody::Right(b) => Pin::new(b).poll_trailers(cx).map_err(Into::into),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            EitherBody::Left(b) => b.is_end_stream(),
            EitherBody::Right(b) => b.is_end_stream(),
        }
    }
}

fn map_option_err<T, U: Into<Error>>(
    err: Option<Result<T, U>>,
) -> Option<Result<T, Error>> {
    err.map(|e| e.map_err(Into::into))
}

fn with_data_server(
    db: SchedulerServer,
) -> impl Filter<Extract = (SchedulerServer,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db.clone())
}

pub fn get_routes(scheduler_server: SchedulerServer) -> BoxedFilter<(impl Reply,)> {
    let routes = warp::path("state")
        .and(with_data_server(scheduler_server))
        .and_then(handlers::scheduler_state);
    routes.boxed()
}
