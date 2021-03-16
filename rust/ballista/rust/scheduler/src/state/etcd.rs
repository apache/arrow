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

//! Etcd config backend.

use std::time::Duration;

use crate::state::ConfigBackendClient;
use ballista_core::error::{ballista_error, Result};

use etcd_client::{GetOptions, LockResponse, PutOptions};
use log::warn;

use super::Lock;

/// A [`ConfigBackendClient`] implementation that uses etcd to save cluster configuration.
#[derive(Clone)]
pub struct EtcdClient {
    etcd: etcd_client::Client,
}

impl EtcdClient {
    pub fn new(etcd: etcd_client::Client) -> Self {
        Self { etcd }
    }
}

#[tonic::async_trait]
impl ConfigBackendClient for EtcdClient {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .etcd
            .clone()
            .get(key, None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .etcd
            .clone()
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| (kv.key_str().unwrap().to_owned(), kv.value().to_owned()))
            .collect())
    }

    async fn put(&self, key: String, value: Vec<u8>, lease_time: Option<Duration>) -> Result<()> {
        let mut etcd = self.etcd.clone();
        let put_options = if let Some(lease_time) = lease_time {
            etcd.lease_grant(lease_time.as_secs() as i64, None)
                .await
                .map(|lease| Some(PutOptions::new().with_lease(lease.id())))
                .map_err(|e| {
                    warn!("etcd lease grant failed: {:?}", e.to_string());
                    ballista_error("etcd lease grant failed")
                })?
        } else {
            None
        };
        etcd.put(key.clone(), value.clone(), put_options)
            .await
            .map_err(|e| {
                warn!("etcd put failed: {}", e);
                ballista_error("etcd put failed")
            })
            .map(|_| ())
    }

    async fn lock(&self) -> Result<Box<dyn Lock>> {
        let mut etcd = self.etcd.clone();
        let lock = etcd
            .lock("/ballista_global_lock", None)
            .await
            .map_err(|e| {
                warn!("etcd lock failed: {}", e);
                ballista_error("etcd lock failed")
            })?;
        Ok(Box::new(EtcdLockGuard { etcd, lock }))
    }
}

struct EtcdLockGuard {
    etcd: etcd_client::Client,
    lock: LockResponse,
}

// Cannot use Drop because we need this to be async
#[tonic::async_trait]
impl Lock for EtcdLockGuard {
    async fn unlock(&mut self) {
        self.etcd.unlock(self.lock.key()).await.unwrap();
    }
}
