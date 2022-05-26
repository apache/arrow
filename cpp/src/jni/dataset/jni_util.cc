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

#include "jni/dataset/jni_util.h"

#include <memory>
#include <mutex>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {
namespace jni {

class ReservationListenableMemoryPool::Impl {
 public:
  explicit Impl(arrow::MemoryPool* pool, std::shared_ptr<ReservationListener> listener,
                int64_t block_size)
      : pool_(pool),
        listener_(listener),
        block_size_(block_size),
        blocks_reserved_(0),
        bytes_reserved_(0) {}

  arrow::Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(UpdateReservation(size));
    arrow::Status error = pool_->Allocate(size, out);
    if (!error.ok()) {
      RETURN_NOT_OK(UpdateReservation(-size));
      return error;
    }
    return arrow::Status::OK();
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    bool reserved = false;
    int64_t diff = new_size - old_size;
    if (new_size >= old_size) {
      // new_size >= old_size, pre-reserve bytes from listener before allocating
      // from underlying pool
      RETURN_NOT_OK(UpdateReservation(diff));
      reserved = true;
    }
    arrow::Status error = pool_->Reallocate(old_size, new_size, ptr);
    if (!error.ok()) {
      if (reserved) {
        // roll back reservations on error
        RETURN_NOT_OK(UpdateReservation(-diff));
      }
      return error;
    }
    if (!reserved) {
      // otherwise (e.g. new_size < old_size), make updates after calling underlying pool
      RETURN_NOT_OK(UpdateReservation(diff));
    }
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    // FIXME: See ARROW-11143, currently method ::Free doesn't allow Status return
    arrow::Status s = UpdateReservation(-size);
    if (!s.ok()) {
      ARROW_LOG(FATAL) << "Failed to update reservation while freeing bytes: "
                       << s.message();
      return;
    }
  }

  arrow::Status UpdateReservation(int64_t diff) {
    int64_t granted = Reserve(diff);
    if (granted == 0) {
      return arrow::Status::OK();
    }
    if (granted < 0) {
      RETURN_NOT_OK(listener_->OnRelease(-granted));
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(listener_->OnReservation(granted));
    return arrow::Status::OK();
  }

  int64_t Reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    bytes_reserved_ += diff;
    int64_t new_block_count;
    if (bytes_reserved_ == 0) {
      new_block_count = 0;
    } else {
      // ceil to get the required block number
      new_block_count = (bytes_reserved_ - 1) / block_size_ + 1;
    }
    int64_t bytes_granted = (new_block_count - blocks_reserved_) * block_size_;
    blocks_reserved_ = new_block_count;
    return bytes_granted;
  }

  int64_t bytes_allocated() { return pool_->bytes_allocated(); }

  int64_t max_memory() { return pool_->max_memory(); }

  std::string backend_name() { return pool_->backend_name(); }

  std::shared_ptr<ReservationListener> get_listener() { return listener_; }

 private:
  arrow::MemoryPool* pool_;
  std::shared_ptr<ReservationListener> listener_;
  int64_t block_size_;
  int64_t blocks_reserved_;
  int64_t bytes_reserved_;
  std::mutex mutex_;
};

ReservationListenableMemoryPool::ReservationListenableMemoryPool(
    MemoryPool* pool, std::shared_ptr<ReservationListener> listener, int64_t block_size) {
  impl_.reset(new Impl(pool, listener, block_size));
}

arrow::Status ReservationListenableMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

arrow::Status ReservationListenableMemoryPool::Reallocate(int64_t old_size,
                                                          int64_t new_size,
                                                          uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ReservationListenableMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ReservationListenableMemoryPool::bytes_allocated() const {
  return impl_->bytes_allocated();
}

int64_t ReservationListenableMemoryPool::max_memory() const {
  return impl_->max_memory();
}

std::string ReservationListenableMemoryPool::backend_name() const {
  return impl_->backend_name();
}

std::shared_ptr<ReservationListener> ReservationListenableMemoryPool::get_listener() {
  return impl_->get_listener();
}

ReservationListenableMemoryPool::~ReservationListenableMemoryPool() {}

Status CheckException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    return Status::Invalid("Error during calling Java code from native code");
  }
  return Status::OK();
}

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

arrow::Result<jmethodID> GetMethodID(JNIEnv* env, jclass this_class, const char* name,
                                     const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    return arrow::Status::Invalid(error_message);
  }
  return ret;
}

arrow::Result<jmethodID> GetStaticMethodID(JNIEnv* env, jclass this_class,
                                           const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find static method " + std::string(name) +
                                " within signature" + std::string(sig);
    return arrow::Status::Invalid(error_message);
  }
  return ret;
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  if (string == nullptr) {
    return std::string();
  }
  const char* chars = env->GetStringUTFChars(string, nullptr);
  std::string ret(chars);
  env->ReleaseStringUTFChars(string, chars);
  return ret;
}

std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array) {
  int length = env->GetArrayLength(str_array);
  std::vector<std::string> vector;
  for (int i = 0; i < length; i++) {
    auto string = reinterpret_cast<jstring>(env->GetObjectArrayElement(str_array, i));
    vector.push_back(JStringToCString(env, string));
  }
  return vector;
}

arrow::Result<jbyteArray> ToSchemaByteArray(JNIEnv* env,
                                            std::shared_ptr<arrow::Schema> schema) {
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::Buffer> buffer,
      arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool()))

  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

arrow::Result<std::shared_ptr<arrow::Schema>> FromSchemaByteArray(
    JNIEnv* env, jbyteArray schemaBytes) {
  arrow::ipc::DictionaryMemo in_memo;
  int schemaBytes_len = env->GetArrayLength(schemaBytes);
  jbyte* schemaBytes_data = env->GetByteArrayElements(schemaBytes, nullptr);
  auto serialized_schema = std::make_shared<arrow::Buffer>(
      reinterpret_cast<uint8_t*>(schemaBytes_data), schemaBytes_len);
  arrow::io::BufferReader buf_reader(serialized_schema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                        arrow::ipc::ReadSchema(&buf_reader, &in_memo))
  env->ReleaseByteArrayElements(schemaBytes, schemaBytes_data, JNI_ABORT);
  return schema;
}
arrow::Status ExportRecordBatch(JNIEnv* env, const std::shared_ptr<RecordBatch>& batch,
                                jlong struct_array) {
  return arrow::ExportRecordBatch(*batch,
                                  reinterpret_cast<struct ArrowArray*>(struct_array));
}

arrow::Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(
    JNIEnv* env, const std::shared_ptr<Schema>& schema, jlong struct_array) {
  return arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(struct_array),
                                  schema);
}

}  // namespace jni
}  // namespace dataset
}  // namespace arrow
