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

#include <arrow/util/memory.h>
#include <sys/socket.h>
#include <utility>

#include "plasma/external_store_worker.h"
#include "plasma/plasma.h"

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string& external_store_endpoint,
                                         const std::string& plasma_store_socket,
                                         size_t parallelism)
    : valid_(external_store != nullptr),
      plasma_store_socket_(plasma_store_socket),
      plasma_clients_(parallelism, nullptr),
      parallelism_(parallelism),
      external_store_endpoint_(external_store_endpoint),
      external_store_(external_store),
      sync_handle_(nullptr),
      async_handles_(parallelism, nullptr),
      terminate_(false),
      stopped_(false),
      num_writes_(0),
      num_bytes_written_(0),
      num_reads_not_found_(0),
      num_reads_(0),
      num_bytes_read_(0) {
  if (valid_) {
    for (size_t i = 0; i < parallelism_; ++i) {
      thread_pool_.emplace_back(&ExternalStoreWorker::DoWork, this, i);
    }
  }
}

ExternalStoreWorker::~ExternalStoreWorker() {
  PrintCounters();
  if (!stopped_) {
    Shutdown();
  }
}

void ExternalStoreWorker::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    terminate_ = true;
  }

  tasks_cv_.notify_all();
  for (std::thread& thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  stopped_ = true;
}

bool ExternalStoreWorker::IsValid() const { return valid_; }

size_t ExternalStoreWorker::Parallelism() { return parallelism_; }

Status ExternalStoreWorker::PutSync(
    const std::vector<ObjectID>& object_ids,
    const std::vector<std::shared_ptr<Buffer>>& object_data) {
  std::shared_ptr<ExternalStoreHandle> handle;
  RETURN_NOT_OK(SyncHandle(&handle));
  RETURN_NOT_OK(handle->Put(object_ids, object_data));

  num_writes_ += object_data.size();
  for (const auto& i : object_data) {
    num_bytes_written_ += i->size();
  }

  return Status::OK();
}

Status ExternalStoreWorker::PutAsync(
    const std::vector<ObjectID>& object_ids,
    const std::vector<std::shared_ptr<Buffer>>& object_data, int* fd) {
  int fds[2];
  ARROW_CHECK(socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) == 0);

  *fd = fds[0];
  int write_fd = fds[1];

  std::shared_ptr<ExternalStoreHandle> handle;
  RETURN_NOT_OK(WriteHandle(&handle));

  std::thread([write_fd, object_ids, object_data, handle, this]() {
    ARROW_CHECK_OK(handle->Put(object_ids, object_data));
    ARROW_CHECK(write(write_fd, "\0", 1) == 1);

    num_writes_ += object_data.size();
    for (const auto& i : object_data) {
      num_bytes_written_ += i->size();
    }

    close(write_fd);
  })
      .detach();

  return Status::OK();
}

Status ExternalStoreWorker::Get(const std::vector<ObjectID>& object_ids,
                                std::vector<std::string>& object_data) {
  std::shared_ptr<ExternalStoreHandle> handle;
  RETURN_NOT_OK(SyncHandle(&handle));
  return Get(handle, object_ids, object_data);
}

Status ExternalStoreWorker::EnqueueUnevictRequest(const ObjectID& object_id) {
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    if (object_ids_.size() >= kMaxEnqueue) {
      return Status::CapacityError("Request queue full");
    }
    object_ids_.push_back(object_id);
  }
  tasks_cv_.notify_one();
  return Status::OK();
}

void ExternalStoreWorker::CopyBuffer(uint8_t* dst, const uint8_t* src, size_t n) {
  if (n > kObjectSizeThreshold) {
    arrow::internal::parallel_memcopy(dst, src, static_cast<int64_t>(n), kCopyBlockSize,
                                      kCopyParallelism);
  } else {
    std::memcpy(dst, src, n);
  }
}

void ExternalStoreWorker::PrintCounters() {
  // Print statistics
  ARROW_LOG(INFO) << "External Store Counters: ";
  ARROW_LOG(INFO) << "Number of objects written: " << num_writes_;
  ARROW_LOG(INFO) << "Number of bytes written: " << num_bytes_written_;
  ARROW_LOG(INFO) << "Number of objects read: " << num_reads_;
  ARROW_LOG(INFO) << "Number of bytes read: " << num_bytes_read_;
  ARROW_LOG(INFO) << "Number of failed reads: " << num_reads_not_found_;
}

Status ExternalStoreWorker::Get(std::shared_ptr<ExternalStoreHandle> handle,
                                const std::vector<ObjectID>& object_ids,
                                std::vector<std::string>& object_data) {
  RETURN_NOT_OK(handle->Get(object_ids, object_data));

  for (const auto& i : object_data) {
    if (i.empty()) {
      num_reads_not_found_++;
      continue;
    }
    num_reads_++;
    num_bytes_read_ += i.size();
  }

  return Status::OK();
}

void ExternalStoreWorker::DoWork(size_t idx) {
  while (true) {
    std::vector<ObjectID> object_ids;
    {
      std::unique_lock<std::mutex> lock(tasks_mutex_);

      // Wait for ObjectIds to become available
      tasks_cv_.wait(lock, [this] { return !object_ids_.empty() || terminate_; });

      // Stop execution if termination signal has been set and there are no
      // more object IDs to process
      if (terminate_ && object_ids_.empty()) {
        return;
      }

      // Create a copy of object IDs to avoid blocking
      object_ids = object_ids_;
      object_ids_.clear();
    }
    tasks_cv_.notify_one();

    std::shared_ptr<ExternalStoreHandle> handle;
    std::shared_ptr<PlasmaClient> client;
    std::vector<std::string> object_data;

    ARROW_CHECK_OK(Client(idx, &client));
    ARROW_CHECK_OK(AsyncHandle(idx, &handle));
    ARROW_CHECK_OK(Get(handle, object_ids, object_data));
    ARROW_CHECK_OK(WriteToPlasma(client, object_ids, object_data));
  }
}

Status ExternalStoreWorker::WriteToPlasma(std::shared_ptr<PlasmaClient> client,
                                          const std::vector<ObjectID>& object_ids,
                                          const std::vector<std::string>& data) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    if (data.at(i).empty()) {
      continue;
    }
    std::shared_ptr<Buffer> object_data;
    auto data_size = static_cast<int64_t>(data.at(i).size());
    auto s = client->Create(object_ids.at(i), data_size, nullptr, 0, &object_data);
    if (s.IsPlasmaObjectExists()) {
      ARROW_LOG(WARNING) << "ObjectID " << object_ids.at(i).hex()
                         << " already exists in Plasma";
      continue;
    }
    CopyBuffer(object_data->mutable_data(),
               reinterpret_cast<const uint8_t*>(data[i].data()),
               static_cast<size_t>(data_size));
    RETURN_NOT_OK(client->Seal(object_ids.at(i), false));
    RETURN_NOT_OK(client->Release(object_ids.at(i)));
    num_reads_++;
  }
  return Status::OK();
}

Status ExternalStoreWorker::Client(size_t idx, std::shared_ptr<PlasmaClient>* client) {
  if (plasma_clients_[idx] == nullptr) {
    plasma_clients_[idx] = std::make_shared<PlasmaClient>();
    RETURN_NOT_OK(plasma_clients_[idx]->Connect(plasma_store_socket_, ""));
  }
  *client = plasma_clients_[idx];
  return Status::OK();
}

Status ExternalStoreWorker::AsyncHandle(size_t idx,
                                        std::shared_ptr<ExternalStoreHandle>* handle) {
  if (async_handles_[idx] == nullptr) {
    RETURN_NOT_OK(
        external_store_->Connect(external_store_endpoint_, &async_handles_[idx]));
  }
  *handle = async_handles_[idx];
  return Status::OK();
}

Status ExternalStoreWorker::SyncHandle(std::shared_ptr<ExternalStoreHandle>* handle) {
  if (sync_handle_ == nullptr) {
    RETURN_NOT_OK(external_store_->Connect(external_store_endpoint_, &sync_handle_));
  }
  *handle = sync_handle_;
  return Status::OK();
}

Status ExternalStoreWorker::WriteHandle(std::shared_ptr<ExternalStoreHandle>* handle) {
  if (write_handle_ == nullptr) {
    RETURN_NOT_OK(external_store_->Connect(external_store_endpoint_, &write_handle_));
  }
  *handle = write_handle_;
  return Status::OK();
}

}  // namespace plasma
