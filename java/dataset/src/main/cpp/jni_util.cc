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

#include "jni_util.h"

#include <memory>
#include <mutex>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {
namespace jni {

jint JNI_VERSION = JNI_VERSION_1_6;

class ReservationListenableMemoryPool::Impl {
 public:
  explicit Impl(arrow::MemoryPool* pool, std::shared_ptr<ReservationListener> listener,
                int64_t block_size)
      : pool_(pool), listener_(listener), block_size_(block_size), blocks_reserved_(0) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) {
    RETURN_NOT_OK(UpdateReservation(size));
    arrow::Status error = pool_->Allocate(size, alignment, out);
    if (!error.ok()) {
      RETURN_NOT_OK(UpdateReservation(-size));
      return error;
    }
    return arrow::Status::OK();
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                           uint8_t** ptr) {
    bool reserved = false;
    int64_t diff = new_size - old_size;
    if (new_size >= old_size) {
      // new_size >= old_size, pre-reserve bytes from listener before allocating
      // from underlying pool
      RETURN_NOT_OK(UpdateReservation(diff));
      reserved = true;
    }
    arrow::Status error = pool_->Reallocate(old_size, new_size, alignment, ptr);
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

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) {
    pool_->Free(buffer, size, alignment);
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
    stats_.UpdateAllocatedBytes(diff);
    int64_t new_block_count;
    int64_t bytes_reserved = stats_.bytes_allocated();
    if (bytes_reserved == 0) {
      new_block_count = 0;
    } else {
      // ceil to get the required block number
      new_block_count = (bytes_reserved - 1) / block_size_ + 1;
    }
    int64_t bytes_granted = (new_block_count - blocks_reserved_) * block_size_;
    blocks_reserved_ = new_block_count;
    return bytes_granted;
  }

  int64_t bytes_allocated() { return stats_.bytes_allocated(); }

  int64_t max_memory() { return stats_.max_memory(); }

  int64_t total_bytes_allocated() { return stats_.total_bytes_allocated(); }

  int64_t num_allocations() { return stats_.num_allocations(); }

  std::string backend_name() { return pool_->backend_name(); }

  std::shared_ptr<ReservationListener> get_listener() { return listener_; }

 private:
  arrow::MemoryPool* pool_;
  std::shared_ptr<ReservationListener> listener_;
  int64_t block_size_;
  int64_t blocks_reserved_;
  arrow::internal::MemoryPoolStats stats_;
  std::mutex mutex_;
};

ReservationListenableMemoryPool::ReservationListenableMemoryPool(
    MemoryPool* pool, std::shared_ptr<ReservationListener> listener, int64_t block_size) {
  impl_.reset(new Impl(pool, listener, block_size));
}

arrow::Status ReservationListenableMemoryPool::Allocate(int64_t size, int64_t alignment,
                                                        uint8_t** out) {
  return impl_->Allocate(size, alignment, out);
}

arrow::Status ReservationListenableMemoryPool::Reallocate(int64_t old_size,
                                                          int64_t new_size,
                                                          int64_t alignment,
                                                          uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, alignment, ptr);
}

void ReservationListenableMemoryPool::Free(uint8_t* buffer, int64_t size,
                                           int64_t alignment) {
  return impl_->Free(buffer, size, alignment);
}

int64_t ReservationListenableMemoryPool::bytes_allocated() const {
  return impl_->bytes_allocated();
}

int64_t ReservationListenableMemoryPool::max_memory() const {
  return impl_->max_memory();
}

int64_t ReservationListenableMemoryPool::total_bytes_allocated() const {
  return impl_->total_bytes_allocated();
}

int64_t ReservationListenableMemoryPool::num_allocations() const {
  return impl_->num_allocations();
}

std::string ReservationListenableMemoryPool::backend_name() const {
  return impl_->backend_name();
}

std::shared_ptr<ReservationListener> ReservationListenableMemoryPool::get_listener() {
  return impl_->get_listener();
}

ReservationListenableMemoryPool::~ReservationListenableMemoryPool() {}

std::string Describe(JNIEnv* env, jthrowable t) {
  jclass describer_class =
      env->FindClass("org/apache/arrow/dataset/jni/JniExceptionDescriber");
  DCHECK_NE(describer_class, nullptr);
  jmethodID describe_method = env->GetStaticMethodID(
      describer_class, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");
  std::string description = JStringToCString(
      env, (jstring)env->CallStaticObjectMethod(describer_class, describe_method, t));
  return description;
}

bool IsErrorInstanceOf(JNIEnv* env, jthrowable t, std::string class_name) {
  jclass jclass = env->FindClass(class_name.c_str());
  DCHECK_NE(jclass, nullptr) << "Could not find Java class " << class_name;
  return env->IsInstanceOf(t, jclass);
}

arrow::StatusCode MapJavaError(JNIEnv* env, jthrowable t) {
  StatusCode code;
  if (IsErrorInstanceOf(env, t, "org/apache/arrow/memory/OutOfMemoryException")) {
    code = StatusCode::OutOfMemory;
  } else if (IsErrorInstanceOf(env, t, "java/lang/UnsupportedOperationException")) {
    code = StatusCode::NotImplemented;
  } else if (IsErrorInstanceOf(env, t, "java/io/NotSerializableException")) {
    code = StatusCode::SerializationError;
  } else if (IsErrorInstanceOf(env, t, "java/io/IOException")) {
    code = StatusCode::IOError;
  } else if (IsErrorInstanceOf(env, t, "java/lang/IllegalArgumentException")) {
    code = StatusCode::Invalid;
  } else if (IsErrorInstanceOf(env, t, "java/lang/IllegalStateException")) {
    code = StatusCode::Invalid;
  } else {
    code = StatusCode::UnknownError;
  }
  return code;
}

JNIEnv* GetEnvOrAttach(JavaVM* vm) {
  JNIEnv* env;
  int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  if (getEnvStat == JNI_EDETACHED) {
    // Reattach current thread to JVM
    getEnvStat = vm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (getEnvStat != JNI_OK) {
      ARROW_LOG(FATAL) << "Failed to attach current thread to JVM";
    }
  }
  return env;
}

const char kJavaErrorDetailTypeId[] = "arrow::dataset::jni::JavaErrorDetail";

JavaErrorDetail::JavaErrorDetail(JavaVM* vm, jthrowable cause) : vm_(vm) {
  JNIEnv* env = GetEnvOrAttach(vm_);
  if (env == nullptr) {
    this->cause_ = nullptr;
    return;
  }
  this->cause_ = (jthrowable)env->NewGlobalRef(cause);
}

JavaErrorDetail::~JavaErrorDetail() {
  JNIEnv* env = GetEnvOrAttach(vm_);
  if (env == nullptr || this->cause_ == nullptr) {
    return;
  }
  env->DeleteGlobalRef(cause_);
}

const char* JavaErrorDetail::type_id() const { return kJavaErrorDetailTypeId; }

jthrowable JavaErrorDetail::GetCause() const {
  JNIEnv* env = GetEnvOrAttach(vm_);
  if (env == nullptr || this->cause_ == nullptr) {
    return nullptr;
  }
  return (jthrowable)env->NewLocalRef(cause_);
}

std::string JavaErrorDetail::ToString() const {
  JNIEnv* env = GetEnvOrAttach(vm_);
  if (env == nullptr) {
    return "Java Exception, ID: " + std::to_string(reinterpret_cast<uintptr_t>(cause_));
  }
  return "Java Exception: " + Describe(env, cause_);
}

Status CheckException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    jthrowable t = env->ExceptionOccurred();
    env->ExceptionClear();
    arrow::StatusCode code = MapJavaError(env, t);
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      return Status::Invalid("Error getting JavaVM object");
    }
    std::shared_ptr<JavaErrorDetail> detail = std::make_shared<JavaErrorDetail>(vm, t);
    return {code, detail->ToString(), detail};
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

  jbyteArray out = env->NewByteArray(static_cast<int>(buffer->size()));
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, static_cast<int>(buffer->size()), src);
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
