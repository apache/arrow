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

//! Etcd config backend.

use std::{task::Poll, time::Duration};

use crate::state::ConfigBackendClient;
use ballista_core::error::{ballista_error, Result};

use etcd_client::{
    GetOptions, LockResponse, PutOptions, WatchOptions, WatchStream, Watcher,
};
use futures::{Stream, StreamExt};
use log::warn;

use super::{Lock, Watch, WatchEvent};

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

    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        lease_time: Option<Duration>,
    ) -> Result<()> {
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

    async fn watch(&self, prefix: String) -> Result<Box<dyn Watch>> {
        let mut etcd = self.etcd.clone();
        let options = WatchOptions::new().with_prefix();
        let (watcher, stream) = etcd.watch(prefix, Some(options)).await.map_err(|e| {
            warn!("etcd watch failed: {}", e);
            ballista_error("etcd watch failed")
        })?;
        Ok(Box::new(EtcdWatch {
            watcher,
            stream,
            buffered_events: Vec::new(),
        }))
    }
}

struct EtcdWatch {
    watcher: Watcher,
    stream: WatchStream,
    buffered_events: Vec<WatchEvent>,
}

#[tonic::async_trait]
impl Watch for EtcdWatch {
    async fn cancel(&mut self) -> Result<()> {
        self.watcher.cancel().await.map_err(|e| {
            warn!("etcd watch cancel failed: {}", e);
            ballista_error("etcd watch cancel failed")
        })
    }
}

impl Stream for EtcdWatch {
    type Item = WatchEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();
        if let Some(event) = self_mut.buffered_events.pop() {
            Poll::Ready(Some(event))
        } else {
            loop {
                match self_mut.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Err(e))) => {
                        warn!("Error when watching etcd prefix: {}", e);
                        continue;
                    }
                    Poll::Ready(Some(Ok(v))) => {
                        self_mut.buffered_events.extend(v.events().iter().map(|ev| {
                            match ev.event_type() {
                                etcd_client::EventType::Put => {
                                    let kv = ev.kv().unwrap();
                                    WatchEvent::Put(
                                        kv.key_str().unwrap().to_string(),
                                        kv.value().to_owned(),
                                    )
                                }
                                etcd_client::EventType::Delete => {
                                    let kv = ev.kv().unwrap();
                                    WatchEvent::Delete(kv.key_str().unwrap().to_string())
                                }
                            }
                        }));
                        if let Some(event) = self_mut.buffered_events.pop() {
                            return Poll::Ready(Some(event));
                        } else {
                            continue;
                        }
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
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
