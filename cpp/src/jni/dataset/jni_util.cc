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
#include "arrow/ipc/metadata_internal.h"
#include "arrow/util/base64.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

#include <memory>
#include <mutex>

#include <flatbuffers/flatbuffers.h>

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

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

Status SetMetadataForSingleField(std::shared_ptr<ArrayData> array_data,
                                 std::vector<ipc::internal::FieldMetadata>& nodes_meta,
                                 std::vector<ipc::internal::BufferMetadata>& buffers_meta,
                                 std::shared_ptr<KeyValueMetadata>& custom_metadata) {
  nodes_meta.push_back({array_data->length, array_data->null_count, 0L});

  for (size_t i = 0; i < array_data->buffers.size(); i++) {
    auto buffer = array_data->buffers.at(i);
    uint8_t* data = nullptr;
    int64_t size = 0;
    if (buffer != nullptr) {
      data = (uint8_t*)buffer->data();
      size = buffer->size();
    }
    ipc::internal::BufferMetadata buffer_metadata{};
    buffer_metadata.offset = reinterpret_cast<int64_t>(data);
    buffer_metadata.length = size;
    // store buffer refs into custom metadata
    jlong ref = CreateNativeRef(buffer);
    custom_metadata->Append(
        "NATIVE_BUFFER_REF_" + std::to_string(i),
        util::base64_encode(reinterpret_cast<unsigned char*>(&ref), sizeof(ref)));
    buffers_meta.push_back(buffer_metadata);
  }

  auto children_data = array_data->child_data;
  for (const auto& child_data : children_data) {
    RETURN_NOT_OK(
        SetMetadataForSingleField(child_data, nodes_meta, buffers_meta, custom_metadata));
  }
  return Status::OK();
}

Result<std::shared_ptr<Buffer>> SerializeMetadata(const RecordBatch& batch,
                                                  const ipc::IpcWriteOptions& options) {
  std::vector<ipc::internal::FieldMetadata> nodes;
  std::vector<ipc::internal::BufferMetadata> buffers;
  std::shared_ptr<KeyValueMetadata> custom_metadata =
      std::make_shared<KeyValueMetadata>();
  for (const auto& column : batch.columns()) {
    auto array_data = column->data();
    RETURN_NOT_OK(SetMetadataForSingleField(array_data, nodes, buffers, custom_metadata));
  }
  std::shared_ptr<Buffer> meta_buffer;
  RETURN_NOT_OK(ipc::internal::WriteRecordBatchMessage(
      batch.num_rows(), 0L, custom_metadata, nodes, buffers, options, &meta_buffer));
  // no message body is needed for JNI serialization/deserialization
  int32_t meta_length = -1;
  ARROW_ASSIGN_OR_RAISE(auto stream, io::BufferOutputStream::Create(1024L));
  RETURN_NOT_OK(ipc::WriteMessage(*meta_buffer, options, stream.get(), &meta_length));
  return stream->Finish();
}

Result<jbyteArray> SerializeUnsafeFromNative(JNIEnv* env,
                                             const std::shared_ptr<RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto meta_buffer,
                        SerializeMetadata(*batch, ipc::IpcWriteOptions::Defaults()));

  jbyteArray ret = env->NewByteArray(meta_buffer->size());
  auto src = reinterpret_cast<const jbyte*>(meta_buffer->data());
  env->SetByteArrayRegion(ret, 0, meta_buffer->size(), src);
  return ret;
}

Result<std::shared_ptr<ArrayData>> MakeArrayData(
    JNIEnv* env, const flatbuf::RecordBatch& batch_meta,
    const std::shared_ptr<const KeyValueMetadata>& custom_metadata,
    const std::shared_ptr<DataType>& type, int32_t* field_offset,
    int32_t* buffer_offset) {
  const org::apache::arrow::flatbuf::FieldNode* field =
      batch_meta.nodes()->Get((*field_offset)++);
  int32_t own_buffer_size = static_cast<int32_t>(type->layout().buffers.size());
  std::vector<std::shared_ptr<Buffer>> buffers;
  for (int32_t i = *buffer_offset; i < *buffer_offset + own_buffer_size; i++) {
    const org::apache::arrow::flatbuf::Buffer* java_managed_buffer =
        batch_meta.buffers()->Get(i);
    const std::string& cleaner_object_ref_base64 =
        util::base64_decode(custom_metadata->value(i * 2));
    const std::string& cleaner_method_ref_base64 =
        util::base64_decode(custom_metadata->value(i * 2 + 1));
    const auto* cleaner_object_ref =
        reinterpret_cast<const jlong*>(cleaner_object_ref_base64.data());
    const auto* cleaner_method_ref =
        reinterpret_cast<const jlong*>(cleaner_method_ref_base64.data());
    auto buffer = std::make_shared<JavaAllocatedBuffer>(
        env, reinterpret_cast<jobject>(*cleaner_object_ref),
        reinterpret_cast<jmethodID>(*cleaner_method_ref),
        reinterpret_cast<uint8_t*>(java_managed_buffer->offset()),
        java_managed_buffer->length());
    buffers.push_back(buffer);
  }
  (*buffer_offset) += own_buffer_size;
  if (type->num_fields() == 0) {
    return ArrayData::Make(type, field->length(), buffers, field->null_count());
  }
  std::vector<std::shared_ptr<ArrayData>> children_array_data;
  for (const auto& child_field : type->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto child_array_data,
                          MakeArrayData(env, batch_meta, custom_metadata,
                                        child_field->type(), field_offset, buffer_offset))
    children_array_data.push_back(child_array_data);
  }
  return ArrayData::Make(type, field->length(), buffers, children_array_data,
                         field->null_count());
}

Result<std::shared_ptr<RecordBatch>> DeserializeUnsafeFromJava(
    JNIEnv* env, std::shared_ptr<Schema> schema, jbyteArray byte_array) {
  int bytes_len = env->GetArrayLength(byte_array);
  jbyte* byte_data = env->GetByteArrayElements(byte_array, nullptr);
  io::BufferReader meta_reader(reinterpret_cast<const uint8_t*>(byte_data),
                               static_cast<int64_t>(bytes_len));
  ARROW_ASSIGN_OR_RAISE(auto meta_message, ipc::ReadMessage(&meta_reader))
  auto meta_buffer = meta_message->metadata();
  auto custom_metadata = meta_message->custom_metadata();
  const flatbuf::Message* flat_meta = nullptr;
  RETURN_NOT_OK(
      ipc::internal::VerifyMessage(meta_buffer->data(), meta_buffer->size(), &flat_meta));
  auto batch_meta = flat_meta->header_as_RecordBatch();

  // Record batch serialized from java should have two ref IDs per buffer: cleaner object
  // ref and cleaner method ref. The refs are originally of 64bit integer type and encoded
  // within base64.
  if (custom_metadata->size() !=
      static_cast<int32_t>(batch_meta->buffers()->size() * 2)) {
    return Status::SerializationError(
        "Buffer count mismatch between metadata and Java managed refs");
  }

  std::vector<std::shared_ptr<ArrayData>> columns_array_data;
  int32_t field_offset = 0;
  int32_t buffer_offset = 0;
  for (int32_t i = 0; i < schema->num_fields(); i++) {
    auto field = schema->field(i);
    ARROW_ASSIGN_OR_RAISE(auto column_array_data,
                          MakeArrayData(env, *batch_meta, custom_metadata, field->type(),
                                        &field_offset, &buffer_offset))
    columns_array_data.push_back(column_array_data);
  }
  if (field_offset != static_cast<int32_t>(batch_meta->nodes()->size())) {
    return Status::SerializationError(
        "Deserialization failed: Field count is not "
        "as expected based on type layout");
  }
  if (buffer_offset != static_cast<int32_t>(batch_meta->buffers()->size())) {
    return Status::SerializationError(
        "Deserialization failed: Buffer count is not "
        "as expected based on type layout");
  }
  int64_t length = batch_meta->length();
  env->ReleaseByteArrayElements(byte_array, byte_data, JNI_ABORT);
  return RecordBatch::Make(schema, length, columns_array_data);
}

}  // namespace jni
}  // namespace dataset
}  // namespace arrow
