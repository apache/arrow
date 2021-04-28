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

#pragma once

#include "arrow/array.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/type.h"

#include <jni.h>

namespace arrow {
namespace dataset {
namespace jni {

Status CheckException(JNIEnv* env);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

Result<jmethodID> GetMethodID(JNIEnv* env, jclass this_class, const char* name,
                              const char* sig);

Result<jmethodID> GetStaticMethodID(JNIEnv* env, jclass this_class, const char* name,
                                    const char* sig);

std::string JStringToCString(JNIEnv* env, jstring string);

std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array);

Result<jbyteArray> ToSchemaByteArray(JNIEnv* env, std::shared_ptr<Schema> schema);

Result<std::shared_ptr<Schema>> FromSchemaByteArray(JNIEnv* env, jbyteArray schemaBytes);

/// \brief Serialize arrow::RecordBatch to jbyteArray (Java byte array byte[]). For
/// letting Java code manage lifecycles of buffers in the input batch, shared pointer IDs
/// pointing to the buffers are serialized into buffer metadata.
Result<jbyteArray> SerializeUnsafeFromNative(JNIEnv* env,
                                             const std::shared_ptr<RecordBatch>& batch);

/// \brief Deserialize jbyteArray (Java byte array byte[]) to arrow::RecordBatch.
Result<std::shared_ptr<RecordBatch>> DeserializeUnsafeFromJava(
    JNIEnv* env, std::shared_ptr<Schema> schema, jbyteArray byte_array);

/// \brief Create a new shared_ptr on heap from shared_ptr t to prevent
/// the managed object from being garbage-collected.
///
/// \return address of the newly created shared pointer
template <typename T>
jlong CreateNativeRef(std::shared_ptr<T> t) {
  std::shared_ptr<T>* retained_ptr = new std::shared_ptr<T>(t);
  return reinterpret_cast<jlong>(retained_ptr);
}

/// \brief Get the shared_ptr that was derived via function CreateNativeRef.
///
/// \param[in] ref address of the shared_ptr
/// \return the shared_ptr object
template <typename T>
std::shared_ptr<T> RetrieveNativeInstance(jlong ref) {
  std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
  return *retrieved_ptr;
}

/// \brief Destroy a shared_ptr using its memory address.
///
/// \param[in] ref address of the shared_ptr
template <typename T>
void ReleaseNativeRef(jlong ref) {
  std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
  delete retrieved_ptr;
}

/// Listener to act on reservations/unreservations from ReservationListenableMemoryPool.
///
/// Note the memory pool will call this listener only on block-level memory
/// reservation/unreservation is granted. So the invocation parameter "size" is always
/// multiple of block size (by default, 512k) specified in memory pool.
class ReservationListener {
 public:
  virtual ~ReservationListener() = default;

  virtual Status OnReservation(int64_t size) = 0;
  virtual Status OnRelease(int64_t size) = 0;

 protected:
  ReservationListener() = default;
};

/// A memory pool implementation for pre-reserving memory blocks from a
/// customizable listener. This will typically be used when memory allocations
/// have to be subject to another "virtual" resource manager, which just tracks or
/// limits number of bytes of application's overall memory usage. The underlying
/// memory pool will still be responsible for actual malloc/free operations.
class ReservationListenableMemoryPool : public MemoryPool {
 public:
  /// \brief Constructor.
  ///
  /// \param[in] pool the underlying memory pool
  /// \param[in] listener a listener for block-level reservations/releases.
  /// \param[in] block_size size of each block to reserve from the listener
  explicit ReservationListenableMemoryPool(MemoryPool* pool,
                                           std::shared_ptr<ReservationListener> listener,
                                           int64_t block_size = 512 * 1024);

  ~ReservationListenableMemoryPool();

  Status Allocate(int64_t size, uint8_t** out) override;

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  std::string backend_name() const override;

  std::shared_ptr<ReservationListener> get_listener();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace jni
}  // namespace dataset
}  // namespace arrow
