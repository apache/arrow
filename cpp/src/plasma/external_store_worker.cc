#include <utility>
#include <arrow/util/memory.h>

#include "plasma/external_store_worker.h"
#include "plasma/plasma.h"

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string &external_store_endpoint,
                                         const std::string &plasma_store_socket,
                                         size_t parallelism)
    : plasma_store_socket_(plasma_store_socket),
      plasma_clients_(parallelism, nullptr),
      parallelism_(parallelism),
      terminate_(false),
      stopped_(false),
      num_writes_(0),
      num_bytes_written_(0),
      num_reads_not_found_(0),
      num_reads_(0),
      num_bytes_read_(0) {
  if (external_store) {
    valid_ = true;
    sync_handle_ = external_store->Connect(external_store_endpoint);
    for (size_t i = 0; i < parallelism_; ++i) {
      read_handles_.push_back(external_store->Connect(external_store_endpoint));
      write_handles_.push_back(external_store->Connect(external_store_endpoint));
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
  for (std::thread &thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  stopped_ = true;
}

bool ExternalStoreWorker::IsValid() const {
  return valid_;
}

size_t ExternalStoreWorker::Parallelism() {
  return parallelism_;
}

void ExternalStoreWorker::Put(const std::vector<ObjectID> &object_ids,
                              const std::vector<std::string> &object_data) {
  size_t num_objects = object_ids.size();
  const ObjectID *ids_ptr = &object_ids[0];
  const std::string *data_ptr = &object_data[0];

  size_t num_chunks = std::min(parallelism_, num_objects);
  if (num_chunks > 1) {
    size_t chunk_size = num_objects / num_chunks;
    size_t last_chunk_size = num_objects - (chunk_size * (num_chunks - 1));

    std::vector<std::future<Status>> futures;
    for (size_t i = 0; i < num_chunks; ++i) {
      size_t chunk_size_i = i == (num_chunks - 1) ? last_chunk_size : chunk_size;
      futures.push_back(std::async(&ExternalStoreWorker::PutChunk,
                                   write_handles_[i],
                                   chunk_size_i,
                                   ids_ptr + i * chunk_size,
                                   data_ptr + i * chunk_size));
    }

    for (auto &fut: futures) {
      ARROW_CHECK_OK(fut.get());
    }
  } else {
    ARROW_CHECK_OK(ExternalStoreWorker::PutChunk(write_handles_.front(),
                                                 num_objects,
                                                 ids_ptr,
                                                 data_ptr));
  }

  num_writes_ += num_objects;
  for (const auto &i : object_data) {
    num_bytes_written_ += i.size();
  }
}

void ExternalStoreWorker::SequentialGet(const std::vector<ObjectID> &object_ids,
                                        std::vector<std::string> &object_data) {
  Get(sync_handle_, object_ids, object_data);
}

bool ExternalStoreWorker::EnqueueUnevictRequest(const ObjectID &object_id) {
  size_t n_enqueued = 0;
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    if (object_ids_.size() >= kMaxEnqueue) {
      return false;
    }
    object_ids_.push_back(object_id);
    n_enqueued = object_ids_.size();
  }
  tasks_cv_.notify_one();
  ARROW_LOG(DEBUG) << "Enqueued " << n_enqueued << " requests";
  return true;
}

void ExternalStoreWorker::CopyBuffer(uint8_t *dst, const uint8_t *src, size_t n) {
  if (n > kObjectSizeThreshold) {
    arrow::internal::parallel_memcopy(dst, src, static_cast<int64_t>(n), kMemcpyBlockSize, kMemcpyParallelism);
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
  ARROW_LOG(INFO) << "Number of objects attempted to read, but not found: " << num_reads_not_found_;
}

void ExternalStoreWorker::Get(std::shared_ptr<ExternalStoreHandle> handle,
                              const std::vector<ObjectID> &object_ids,
                              std::vector<std::string> &object_data) {
  object_data.resize(object_ids.size());
  ARROW_CHECK_OK(handle->Get(object_ids.size(), &object_ids[0], &object_data[0]));

  for (const auto &i : object_data) {
    if (i.empty()) {
      num_reads_not_found_++;
      continue;
    }
    num_reads_++;
    num_bytes_read_ += i.size();
  }
}

Status ExternalStoreWorker::PutChunk(std::shared_ptr<ExternalStoreHandle> handle,
                                     size_t num_objects,
                                     const ObjectID *ids,
                                     const std::string *data) {
  return handle->Put(num_objects, ids, data);
}

void ExternalStoreWorker::DoWork(size_t thread_id) {
  while (true) {
    std::vector<ObjectID> object_ids;
    {
      std::unique_lock<std::mutex> lock(tasks_mutex_);

      // Wait for ObjectIds to become available
      tasks_cv_.wait(lock, [this] {
        return !object_ids_.empty() || terminate_;
      });

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

    std::vector<std::string> object_data;
    Get(read_handles_[thread_id], object_ids, object_data);
    WriteToPlasma(Client(thread_id), object_ids, object_data);
  }
}

void ExternalStoreWorker::WriteToPlasma(std::shared_ptr<PlasmaClient> client,
                                        const std::vector<ObjectID> &object_ids,
                                        const std::vector<std::string> &data) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    if (data.at(i).empty()) {
      continue;
    }
    std::shared_ptr<Buffer> object_data;
    auto data_size = static_cast<int64_t>(data.at(i).size());
    auto s = client->Create(object_ids.at(i), data_size, nullptr, 0, &object_data);
    if (s.IsPlasmaObjectExists()) {
      ARROW_LOG(WARNING) << "Object " << object_ids.at(i).hex() << " already exists in Plasma";
      continue;
    }
    CopyBuffer(object_data->mutable_data(),
               reinterpret_cast<const uint8_t *>(data[i].data()),
               static_cast<size_t>(data_size));
    ARROW_CHECK_OK(client->SealWithoutNotification(object_ids.at(i)));
    ARROW_CHECK_OK(client->Release(object_ids.at(i)));
    num_reads_++;
  }
}

std::shared_ptr<PlasmaClient> ExternalStoreWorker::Client(size_t thread_id) {
  if (plasma_clients_[thread_id] == nullptr) {
    plasma_clients_[thread_id] = std::make_shared<PlasmaClient>();
    ARROW_CHECK_OK(plasma_clients_[thread_id]->Connect(plasma_store_socket_, ""));
  }
  return plasma_clients_[thread_id];
}

}
