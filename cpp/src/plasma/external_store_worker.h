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

#ifndef EXTERNAL_STORE_WORKER_H
#define EXTERNAL_STORE_WORKER_H

#include <unistd.h>

#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "plasma/common.h"
#include "plasma/external_store.h"

namespace plasma {

// ==== The external store worker ====
//
// The worker maintains a thread-pool internally for servicing Get requests.
// All Get requests are enqueued, and periodically serviced by a worker
// thread. All Put requests are serviced using multiple threads.
// The worker interface ensures thread-safe access to the external store.
//
// Note: this implementation uses a custom thread-pool since it is used in ways
// that the Arrow thread-pool doesn't support:
// 1. The implementation maintains a queue of object IDs to fetch, and fetches
//    multiple object IDs per thread to improve performance via batching.
// 2. The implementation uses the thread-pool to detect if too many Get()
//    requests have been enqueued, and sends an error message back to the
//    caller so that the calling thread can try synchronous Get() instead.

class ExternalStoreWorker {
 public:
  static const size_t kMaxEnqueue = 32;
  static const size_t kCopyParallelism = 4;
  static const size_t kObjectSizeThreshold = 1024 * 1024;
  static const size_t kCopyBlockSize = 64;

  /// Constructor.
  ///
  /// @param external_store The external store implementation.
  /// @param external_store_endpoint The external store endpoint to connect to.
  /// @param plasma_store_socket The socket that Plasma clients can connect to.
  /// @param parallelism The number of threads to use for async requests.
  ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                      const std::string& external_store_endpoint,
                      const std::string& plasma_store_socket, size_t parallelism);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Get the parallelism for the external store worker.
  ///
  /// \return The degree of parallelism for the external store worker.
  size_t Parallelism();

  /// Put objects in the external store. Called synchronously from the caller
  /// thread.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  void Put(const std::vector<ObjectID>& object_ids,
           const std::vector<std::shared_ptr<Buffer>>& object_data);

  /// Get objects from the external store. Called synchronously from the caller
  /// thread.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param[out] object_data The object data to get.
  void Get(const std::vector<ObjectID>& object_ids,
           std::vector<std::string>& object_data);

  /// Copy memory buffer in parallel if data size is large enough.
  ///
  /// \param dst Destination memory buffer.
  /// \param src Source memory buffer.
  /// \param n Number of bytes to copy.
  void CopyBuffer(uint8_t* dst, const uint8_t* src, size_t n);

  /// Enqueue an un-evict request; if the request is successfully enqueued, the
  /// worker thread processes the request, reads the object from external store
  /// and writes it back to plasma.
  ///
  /// \param object_id The object ID corresponding to the un-evict request.
  /// \return True if the request is enqueued successfully, false if there are
  ///         too many requests enqueued already.
  bool EnqueueUnevictRequest(const ObjectID& object_id);

  /// Print Counters
  void PrintCounters();

  /// Shutdown the external store worker.
  void Shutdown();

 private:
  void Get(std::shared_ptr<ExternalStoreHandle> handle,
           const std::vector<ObjectID>& object_ids,
           std::vector<std::string>& object_data);

  /// Contains the logic for a worker thread.
  ///
  /// \param idx The thread ID.
  void DoWork(size_t idx);

  /// Get objects from external store and writes it back to plasma store.
  ///
  /// \param object_ids The object IDs to get.
  void WriteToPlasma(std::shared_ptr<PlasmaClient> client,
                     const std::vector<ObjectID>& object_ids,
                     const std::vector<std::string>& data);

  /// Returns a client to the plasma store for the given thread ID,
  /// creating one if not already initialized.
  ///
  /// \param idx The thread ID.
  /// \return A client to the plasma store.
  std::shared_ptr<PlasmaClient> Client(size_t idx);

  // Whether or not plasma is backed by external store
  bool valid_;

  // Plasma store connection
  std::string plasma_store_socket_;
  std::vector<std::shared_ptr<PlasmaClient>> plasma_clients_;

  // External Store handles
  size_t parallelism_;
  std::shared_ptr<ExternalStoreHandle> sync_handle_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> async_handles_;

  // Worker thread
  std::vector<std::thread> thread_pool_;
  std::mutex tasks_mutex_;
  std::condition_variable tasks_cv_;
  bool terminate_;
  bool stopped_;

  // Enqueued object IDs
  std::vector<ObjectID> object_ids_;

  // External store read/write statistics
  std::atomic_size_t num_writes_;
  std::atomic_size_t num_bytes_written_;
  std::atomic_size_t num_reads_not_found_;
  std::atomic_size_t num_reads_;
  std::atomic_size_t num_bytes_read_;
};

}  // namespace plasma

#endif  // EXTERNAL_STORE_WORKER_H
