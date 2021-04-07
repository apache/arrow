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

#include "arrow/util/logging.h"

#include <memory>
#include <mutex>

namespace arrow {
namespace dataset {
namespace jni {

class ReservationListenableMemoryPool::Impl {
 public:
  explicit Impl(MemoryPool* pool, std::shared_ptr<ReservationListener> listener,
                int64_t block_size)
      : pool_(pool),
        listener_(listener),
        block_size_(block_size),
        blocks_reserved_(0),
        bytes_reserved_(0) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(UpdateReservation(size));
    Status error = pool_->Allocate(size, out);
    if (!error.ok()) {
      RETURN_NOT_OK(UpdateReservation(-size));
      return error;
    }
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    bool reserved = false;
    int64_t diff = new_size - old_size;
    if (new_size >= old_size) {
      // new_size >= old_size, pre-reserve bytes from listener before allocating
      // from underlying pool
      RETURN_NOT_OK(UpdateReservation(diff));
      reserved = true;
    }
    Status error = pool_->Reallocate(old_size, new_size, ptr);
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
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    // FIXME: See ARROW-11143, currently method ::Free doesn't allow Status return
    Status s = UpdateReservation(-size);
    if (!s.ok()) {
      ARROW_LOG(FATAL) << "Failed to update reservation while freeing bytes: "
                       << s.message();
      return;
    }
  }

  Status UpdateReservation(int64_t diff) {
    int64_t granted = Reserve(diff);
    if (granted == 0) {
      return Status::OK();
    }
    if (granted < 0) {
      RETURN_NOT_OK(listener_->OnRelease(-granted));
      return Status::OK();
    }
    RETURN_NOT_OK(listener_->OnReservation(granted));
    return Status::OK();
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
  MemoryPool* pool_;
  std::shared_ptr<ReservationListener> listener_;
  int64_t block_size_;
  int64_t blocks_reserved_;
  int64_t bytes_reserved_;
  std::mutex mutex_;
};

/// \brief Buffer implementation that binds to a
/// Java buffer reference. Java buffer's release
/// method will be called once when being destructed.
class JavaAllocatedBuffer : public Buffer {
 public:
  JavaAllocatedBuffer(JNIEnv* env, jobject cleaner_ref, jmethodID cleaner_method_ref,
                      uint8_t* buffer, int32_t len)
      : Buffer(buffer, len),
        env_(env),
        cleaner_ref_(cleaner_ref),
        cleaner_method_ref_(cleaner_method_ref) {}

  ~JavaAllocatedBuffer() override {
    env_->CallVoidMethod(cleaner_ref_, cleaner_method_ref_);
    env_->DeleteGlobalRef(cleaner_ref_);
  }

 private:
  JNIEnv* env_;
  jobject cleaner_ref_;
  jmethodID cleaner_method_ref_;
};

ReservationListenableMemoryPool::ReservationListenableMemoryPool(
    MemoryPool* pool, std::shared_ptr<ReservationListener> listener, int64_t block_size) {
  impl_.reset(new Impl(pool, listener, block_size));
}

Status ReservationListenableMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ReservationListenableMemoryPool::Reallocate(int64_t old_size, int64_t new_size,
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

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

Result<jmethodID> GetMethodID(JNIEnv* env, jclass this_class, const char* name,
                              const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    return Status::Invalid(error_message);
  }
  return ret;
}

Result<jmethodID> GetStaticMethodID(JNIEnv* env, jclass this_class, const char* name,
                                    const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find static method " + std::string(name) +
                                " within signature" + std::string(sig);
    return Status::Invalid(error_message);
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

Result<jbyteArray> ToSchemaByteArray(JNIEnv* env, std::shared_ptr<Schema> schema) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer,
                        ipc::SerializeSchema(*schema, default_memory_pool()))

  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

Result<std::shared_ptr<Schema>> FromSchemaByteArray(JNIEnv* env, jbyteArray schemaBytes) {
  ipc::DictionaryMemo in_memo;
  int schemaBytes_len = env->GetArrayLength(schemaBytes);
  jbyte* schemaBytes_data = env->GetByteArrayElements(schemaBytes, nullptr);
  auto serialized_schema = std::make_shared<Buffer>(
      reinterpret_cast<uint8_t*>(schemaBytes_data), schemaBytes_len);
  io::BufferReader buf_reader(serialized_schema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                        ipc::ReadSchema(&buf_reader, &in_memo))
  env->ReleaseByteArrayElements(schemaBytes, schemaBytes_data, JNI_ABORT);
  return schema;
}

Status SetSingleField(std::shared_ptr<ArrayData> array_data,
                      UnsafeNativeManagedRecordBatchProto& batch_proto) {
  FieldProto* field_adder = batch_proto.add_fields();
  field_adder->set_length(array_data->length);
  field_adder->set_nullcount(array_data->null_count);

  std::vector<std::shared_ptr<Buffer>> buffers;
  for (const auto& buffer : array_data->buffers) {
    buffers.push_back(buffer);
  }

  for (const auto& buffer : buffers) {
    uint8_t* data = nullptr;
    int64_t size = 0;
    int64_t capacity = 0;
    if (buffer != nullptr) {
      data = (uint8_t*)buffer->data();
      size = buffer->size();
      capacity = buffer->capacity();
    }
    NativeManagedBufferProto* native_managed_buffer = batch_proto.add_buffers();
    native_managed_buffer->set_nativebufferid(CreateNativeRef(buffer));
    native_managed_buffer->set_memoryaddress(reinterpret_cast<uintptr_t>(data));
    native_managed_buffer->set_size(size);
    native_managed_buffer->set_capacity(capacity);
  }
  auto children_data = array_data->child_data;
  for (const auto& child_data : children_data) {
    RETURN_NOT_OK(SetSingleField(child_data, batch_proto));
  }
  return Status::OK();
}

Result<jbyteArray> SerializeUnsafeFromNative(JNIEnv* env,
                                             const std::shared_ptr<RecordBatch>& batch) {
  UnsafeNativeManagedRecordBatchProto batch_proto;
  batch_proto.set_numrows(batch->num_rows());

  for (const auto& column : batch->columns()) {
    auto array_data = column->data();
    RETURN_NOT_OK(SetSingleField(array_data, batch_proto));
  }

  auto size = static_cast<int32_t>(batch_proto.ByteSizeLong());
  std::unique_ptr<jbyte[]> buffer{new jbyte[size]};
  batch_proto.SerializeToArray(reinterpret_cast<void*>(buffer.get()), size);
  jbyteArray ret = env->NewByteArray(size);
  env->SetByteArrayRegion(ret, 0, size, buffer.get());
  return ret;
}

Result<std::shared_ptr<ArrayData>> MakeArrayData(
    JNIEnv* env, const UnsafeJavaManagedRecordBatchProto& batch_proto,
    const std::shared_ptr<DataType>& type, int* field_offset, int* buffer_offset) {
  const FieldProto& field = batch_proto.fields((*field_offset)++);
  int own_buffer_size = static_cast<int>(type->layout().buffers.size());
  std::vector<std::shared_ptr<Buffer>> buffers;
  for (int i = *buffer_offset; i < *buffer_offset + own_buffer_size; i++) {
    const JavaManagedBufferProto& java_managed_buffer = batch_proto.buffers(i);
    auto buffer = std::make_shared<JavaAllocatedBuffer>(
        env, reinterpret_cast<jobject>(java_managed_buffer.cleanerobjectref()),
        reinterpret_cast<jmethodID>(java_managed_buffer.cleanermethodref()),
        reinterpret_cast<uint8_t*>(java_managed_buffer.memoryaddress()),
        java_managed_buffer.size());
    buffers.push_back(buffer);
  }
  (*buffer_offset) += own_buffer_size;
  if (type->num_fields() == 0) {
    return ArrayData::Make(type, field.length(), buffers, field.nullcount());
  }
  std::vector<std::shared_ptr<ArrayData>> children_array_data;
  for (const auto& child_field : type->fields()) {
    ARROW_ASSIGN_OR_RAISE(
        auto child_array_data,
        MakeArrayData(env, batch_proto, child_field->type(), field_offset, buffer_offset))
    children_array_data.push_back(child_array_data);
  }
  return ArrayData::Make(type, field.length(), buffers, children_array_data,
                         field.nullcount());
}

Result<std::shared_ptr<RecordBatch>> DeserializeUnsafeFromJava(
    JNIEnv* env, std::shared_ptr<Schema> schema, jbyteArray data_bytes) {
  int bytes_len = env->GetArrayLength(data_bytes);
  jbyte* bytes_data = env->GetByteArrayElements(data_bytes, nullptr);
  UnsafeJavaManagedRecordBatchProto batch_proto;
  batch_proto.ParseFromArray(bytes_data, bytes_len);
  std::vector<std::shared_ptr<ArrayData>> columns_array_data;
  int field_offset = 0;
  int buffer_offset = 0;
  for (int i = 0; i < schema->num_fields(); i++) {
    auto field = schema->field(i);
    ARROW_ASSIGN_OR_RAISE(
        auto column_array_data,
        MakeArrayData(env, batch_proto, field->type(), &field_offset, &buffer_offset))

    columns_array_data.push_back(column_array_data);
  }
  if (field_offset != batch_proto.fields_size()) {
    return Status::SerializationError(
        "Deserialization failed: Field count is not "
        "as expected based on type layout");
  }
  if (buffer_offset != batch_proto.buffers_size()) {
    return Status::SerializationError(
        "Deserialization failed: Buffer count is not "
        "as expected based on type layout");
  }
  return RecordBatch::Make(schema, batch_proto.numrows(), columns_array_data);
}

}  // namespace jni
}  // namespace dataset
}  // namespace arrow
