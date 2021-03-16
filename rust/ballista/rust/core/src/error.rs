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

//! Ballista error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use sqlparser::parser;

pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    SqlError(parser::ParserError),
    IoError(io::Error),
    // ReqwestError(reqwest::Error),
    //HttpError(http::Error),
    // KubeAPIError(kube::error::Error),
    // KubeAPIRequestError(k8s_openapi::RequestError),
    // KubeAPIResponseError(k8s_openapi::ResponseError),
    TonicError(tonic::transport::Error),
    GrpcError(tonic::Status),
    TokioError(tokio::task::JoinError),
}

impl<T> Into<Result<T>> for BallistaError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

pub fn ballista_error(message: &str) -> BallistaError {
    BallistaError::General(message.to_owned())
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ArrowError> for BallistaError {
    fn from(e: ArrowError) -> Self {
        BallistaError::ArrowError(e)
    }
}

impl From<parser::ParserError> for BallistaError {
    fn from(e: parser::ParserError) -> Self {
        BallistaError::SqlError(e)
    }
}

impl From<DataFusionError> for BallistaError {
    fn from(e: DataFusionError) -> Self {
        BallistaError::DataFusionError(e)
    }
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
    }
}

// impl From<reqwest::Error> for BallistaError {
//     fn from(e: reqwest::Error) -> Self {
//         BallistaError::ReqwestError(e)
//     }
// }
//
// impl From<http::Error> for BallistaError {
//     fn from(e: http::Error) -> Self {
//         BallistaError::HttpError(e)
//     }
// }

// impl From<kube::error::Error> for BallistaError {
//     fn from(e: kube::error::Error) -> Self {
//         BallistaError::KubeAPIError(e)
//     }
// }

// impl From<k8s_openapi::RequestError> for BallistaError {
//     fn from(e: k8s_openapi::RequestError) -> Self {
//         BallistaError::KubeAPIRequestError(e)
//     }
// }

// impl From<k8s_openapi::ResponseError> for BallistaError {
//     fn from(e: k8s_openapi::ResponseError) -> Self {
//         BallistaError::KubeAPIResponseError(e)
//     }
// }

impl From<tonic::transport::Error> for BallistaError {
    fn from(e: tonic::transport::Error) -> Self {
        BallistaError::TonicError(e)
    }
}

impl From<tonic::Status> for BallistaError {
    fn from(e: tonic::Status) -> Self {
        BallistaError::GrpcError(e)
    }
}

impl From<tokio::task::JoinError> for BallistaError {
    fn from(e: tokio::task::JoinError) -> Self {
        BallistaError::TokioError(e)
    }
}

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(ref desc) => write!(f, "Not implemented: {}", desc),
            BallistaError::General(ref desc) => write!(f, "General error: {}", desc),
            BallistaError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            BallistaError::DataFusionError(ref desc) => write!(f, "DataFusion error: {:?}", desc),
            BallistaError::SqlError(ref desc) => write!(f, "SQL error: {:?}", desc),
            BallistaError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            // BallistaError::ReqwestError(ref desc) => write!(f, "Reqwest error: {}", desc),
            // BallistaError::HttpError(ref desc) => write!(f, "HTTP error: {}", desc),
            // BallistaError::KubeAPIError(ref desc) => write!(f, "Kube API error: {}", desc),
            // BallistaError::KubeAPIRequestError(ref desc) => {
            //     write!(f, "KubeAPI request error: {}", desc)
            // }
            // BallistaError::KubeAPIResponseError(ref desc) => {
            //     write!(f, "KubeAPI response error: {}", desc)
            // }
            BallistaError::TonicError(desc) => write!(f, "Tonic error: {}", desc),
            BallistaError::GrpcError(desc) => write!(f, "Grpc error: {}", desc),
            BallistaError::Internal(desc) => write!(f, "Internal Ballista error: {}", desc),
            BallistaError::TokioError(desc) => write!(f, "Tokio join error: {}", desc),
        }
    }
}

impl Error for BallistaError {}
