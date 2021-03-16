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

//! Core executor logic for executing queries and storing results in memory.

pub mod collect;
pub mod flight_service;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    /// Directory for temporary files, such as IPC files
    pub(crate) work_dir: String,
    pub(crate) concurrent_tasks: usize,
}

impl ExecutorConfig {
    pub fn new(host: &str, port: u16, work_dir: &str, concurrent_tasks: usize) -> Self {
        Self {
            host: host.to_owned(),
            port,
            work_dir: work_dir.to_owned(),
            concurrent_tasks,
        }
    }
}

#[allow(dead_code)]
pub struct BallistaExecutor {
    pub(crate) config: ExecutorConfig,
}

impl BallistaExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }
}
