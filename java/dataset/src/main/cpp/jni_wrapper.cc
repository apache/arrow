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

#include <mutex>
#include <utility>
#include <unordered_map>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/relation.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"
#include "jni_util.h"
#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_NativeMemoryPool.h"
#include "org_apache_arrow_dataset_substrait_JniWrapper.h"

namespace {

jclass illegal_access_exception_class;
jclass illegal_argument_exception_class;
jclass runtime_exception_class;

jclass java_reservation_listener_class;

jmethodID reserve_memory_method;
jmethodID unreserve_memory_method;

jlong default_memory_pool_id = -1L;

jint JNI_VERSION = JNI_VERSION_1_6;

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg, jthrowable cause)
      : runtime_error(arg), cause_(cause) {}

  jthrowable GetCause() const { return cause_; }
  bool HasCause() const { return cause_ != nullptr; }

 private:
  jthrowable cause_;
};

void ThrowPendingException(const std::string& message, jthrowable cause = nullptr) {
  throw JniPendingException(message, cause);
}

void ThrowIfError(const arrow::Status& status) {
  const std::shared_ptr<arrow::StatusDetail>& detail = status.detail();
  const std::shared_ptr<const arrow::dataset::jni::JavaErrorDetail>& maybe_java =
      std::dynamic_pointer_cast<const arrow::dataset::jni::JavaErrorDetail>(detail);
  if (maybe_java != nullptr) {
    ThrowPendingException(status.message(), maybe_java->GetCause());
    return;
  }
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

class JNIEnvGuard {
 public:
  explicit JNIEnvGuard(JavaVM* vm) : vm_(vm), env_(nullptr), should_detach_(false) {
    JNIEnv* env;
    jint code = vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (code == JNI_EDETACHED) {
      JavaVMAttachArgs args;
      args.version = JNI_VERSION;
      args.name = NULL;
      args.group = NULL;
      code = vm->AttachCurrentThread(reinterpret_cast<void**>(&env), &args);
      should_detach_ = (code == JNI_OK);
    }
    if (code != JNI_OK) {
      ThrowPendingException("Failed to attach the current thread to a Java VM");
    }
    env_ = env;
  }

  JNIEnv* env() { return env_; }

  ~JNIEnvGuard() {
    if (should_detach_) {
      vm_->DetachCurrentThread();
      should_detach_ = false;
    }
  }

 private:
  JavaVM* vm_;
  JNIEnv* env_;
  bool should_detach_;
};

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  const arrow::Status& status = result.status();
  ThrowIfError(status);
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) { ThrowIfError(status); }

void JniThrow(std::string message) { ThrowPendingException(message); }

arrow::Result<std::shared_ptr<arrow::dataset::FileFormat>> GetFileFormat(
    jint file_format_id) {
  switch (file_format_id) {
    case 0:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    case 1:
      return std::make_shared<arrow::dataset::IpcFileFormat>();
#ifdef ARROW_ORC
    case 2:
      return std::make_shared<arrow::dataset::OrcFileFormat>();
#endif
#ifdef ARROW_CSV
    case 3:
      return std::make_shared<arrow::dataset::CsvFileFormat>();
#endif
#ifdef ARROW_JSON
    case 4:
      return std::make_shared<arrow::dataset::JsonFileFormat>();
#endif
    default:
      std::string error_message =
          "illegal file format id: " + std::to_string(file_format_id);
      return arrow::Status::Invalid(error_message);
  }
}

class ReserveFromJava : public arrow::dataset::jni::ReservationListener {
 public:
  ReserveFromJava(JavaVM* vm, jobject java_reservation_listener)
      : vm_(vm), java_reservation_listener_(java_reservation_listener) {}

  arrow::Status OnReservation(int64_t size) override {
    try {
      JNIEnvGuard guard(vm_);
      JNIEnv* env = guard.env();
      env->CallObjectMethod(java_reservation_listener_, reserve_memory_method, size);
      RETURN_NOT_OK(arrow::dataset::jni::CheckException(env));
      return arrow::Status::OK();
    } catch (const JniPendingException& e) {
      return arrow::Status::Invalid(e.what());
    }
  }

  arrow::Status OnRelease(int64_t size) override {
    try {
      JNIEnvGuard guard(vm_);
      JNIEnv* env = guard.env();
      env->CallObjectMethod(java_reservation_listener_, unreserve_memory_method, size);
      RETURN_NOT_OK(arrow::dataset::jni::CheckException(env));
      return arrow::Status::OK();
    } catch (const JniPendingException& e) {
      return arrow::Status::Invalid(e.what());
    }
  }

  jobject GetJavaReservationListener() { return java_reservation_listener_; }

 private:
  JavaVM* vm_;
  jobject java_reservation_listener_;
};

/// \class DisposableScannerAdaptor
/// \brief An adaptor that iterates over a Scanner instance then returns RecordBatches
/// directly.
///
/// This lessens the complexity of the JNI bridge to make sure it to be easier to
/// maintain. On Java-side, NativeScanner can only produces a single NativeScanTask
/// instance during its whole lifecycle. Each task stands for a DisposableScannerAdaptor
/// instance through JNI bridge.
///
class DisposableScannerAdaptor {
 public:
  DisposableScannerAdaptor(std::shared_ptr<arrow::dataset::Scanner> scanner,
                           arrow::dataset::TaggedRecordBatchIterator batch_itr)
      : scanner_(std::move(scanner)), batch_itr_(std::move(batch_itr)) {}

  static arrow::Result<std::shared_ptr<DisposableScannerAdaptor>> Create(
      std::shared_ptr<arrow::dataset::Scanner> scanner) {
    ARROW_ASSIGN_OR_RAISE(auto batch_itr, scanner->ScanBatches());
    return std::make_shared<DisposableScannerAdaptor>(scanner, std::move(batch_itr));
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, NextBatch());
    return batch;
  }

  const std::shared_ptr<arrow::dataset::Scanner>& GetScanner() const { return scanner_; }

 private:
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::TaggedRecordBatchIterator batch_itr_;

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextBatch() {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_itr_.Next())
    return batch.record_batch;
  }
};

arrow::Result<std::shared_ptr<arrow::Schema>> SchemaFromColumnNames(
    const std::shared_ptr<arrow::Schema>& input,
    const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<arrow::Field>> columns;
  for (arrow::FieldRef ref : column_names) {
    auto maybe_field = ref.GetOne(*input);
    if (maybe_field.ok()) {
      columns.push_back(std::move(maybe_field).ValueOrDie());
    } else {
      return arrow::Status::Invalid("Partition column '", ref.ToString(), "' is not in dataset schema");
    }
  }
  return schema(std::move(columns))->WithMetadata(input->metadata());
}
}  // namespace

using arrow::dataset::jni::CreateGlobalClassReference;
using arrow::dataset::jni::CreateNativeRef;
using arrow::dataset::jni::FromSchemaByteArray;
using arrow::dataset::jni::GetMethodID;
using arrow::dataset::jni::JStringToCString;
using arrow::dataset::jni::ReleaseNativeRef;
using arrow::dataset::jni::RetrieveNativeInstance;
using arrow::dataset::jni::ToSchemaByteArray;
using arrow::dataset::jni::ToStringVector;

using arrow::dataset::jni::ReservationListenableMemoryPool;
using arrow::dataset::jni::ReservationListener;

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
    if (e.HasCause()) {                               \
      env->Throw(e.GetCause());                       \
      return fallback_expr;                           \
    }                                                 \
    env->ThrowNew(runtime_exception_class, e.what()); \
    return fallback_expr;                             \
  }
// macro ended

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  JNI_METHOD_START
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  java_reservation_listener_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/ReservationListener;");
  reserve_memory_method =
      JniGetOrThrow(GetMethodID(env, java_reservation_listener_class, "reserve", "(J)V"));
  unreserve_memory_method = JniGetOrThrow(
      GetMethodID(env, java_reservation_listener_class, "unreserve", "(J)V"));

  default_memory_pool_id = reinterpret_cast<jlong>(arrow::default_memory_pool());
  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(java_reservation_listener_class);

  default_memory_pool_id = -1L;
}

/// Unpack the named tables passed through JNI.
///
/// Named tables are encoded as a string array, where every two elements
/// encode (1) the table name and (2) the address of an ArrowArrayStream
/// containing the table data.  This function will eagerly read all
/// tables into Tables.
std::unordered_map<std::string, std::shared_ptr<arrow::Table>> LoadNamedTables(JNIEnv* env, const jobjectArray& str_array) {
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> map_table_to_record_batch_reader;
  int length = env->GetArrayLength(str_array);
  if (length % 2 != 0) {
    JniThrow("Cannot map odd number of array elements to key/value pairs");
  }
  std::shared_ptr<arrow::Table> output_table;
  for (int pos = 0; pos < length; pos++) {
    auto j_string_key = reinterpret_cast<jstring>(env->GetObjectArrayElement(str_array, pos));
    pos++;
    auto j_string_value = reinterpret_cast<jstring>(env->GetObjectArrayElement(str_array, pos));
    uintptr_t memory_address = 0;
    try {
      memory_address = std::stol(JStringToCString(env, j_string_value));
    } catch(const std::exception& ex) {
      JniThrow("Failed to parse memory address from string value. Error: " + std::string(ex.what()));
    } catch (...) {
      JniThrow("Failed to parse memory address from string value.");
    }
    auto* arrow_stream_in = reinterpret_cast<ArrowArrayStream*>(memory_address);
    std::shared_ptr<arrow::RecordBatchReader> readerIn = JniGetOrThrow(arrow::ImportRecordBatchReader(arrow_stream_in));
    output_table = JniGetOrThrow(readerIn->ToTable());
    map_table_to_record_batch_reader[JStringToCString(env, j_string_key)] = output_table;
  }
  return map_table_to_record_batch_reader;
}

/// Find the arrow Table associated with a given table name
std::shared_ptr<arrow::Table> GetTableByName(const std::vector<std::string>& names,
    const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& tables) {
  if (names.size() != 1) {
    JniThrow("Tables with hierarchical names are not supported");
  }
  const auto& it = tables.find(names[0]);
  if (it == tables.end()) {
    JniThrow("Table is referenced, but not provided: " + names[0]);
  }
  return it->second;
}

std::shared_ptr<arrow::Buffer> LoadArrowBufferFromByteBuffer(JNIEnv* env, jobject byte_buffer) {
  const auto *buff = reinterpret_cast<jbyte*>(env->GetDirectBufferAddress(byte_buffer));
  int length = env->GetDirectBufferCapacity(byte_buffer);
  std::shared_ptr<arrow::Buffer> buffer = JniGetOrThrow(arrow::AllocateBuffer(length));
  std::memcpy(buffer->mutable_data(), buff, length);
  return buffer;
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    getDefaultMemoryPool
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_getDefaultMemoryPool(JNIEnv* env,
                                                                        jclass) {
  JNI_METHOD_START
  return default_memory_pool_id;
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    createListenableMemoryPool
 * Signature: (Lorg/apache/arrow/memory/ReservationListener;)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_createListenableMemoryPool(
    JNIEnv* env, jclass, jobject jlistener) {
  JNI_METHOD_START
  jobject jlistener_ref = env->NewGlobalRef(jlistener);
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<ReservationListener> listener =
      std::make_shared<ReserveFromJava>(vm, jlistener_ref);
  auto memory_pool =
      new ReservationListenableMemoryPool(arrow::default_memory_pool(), listener);
  return reinterpret_cast<jlong>(memory_pool);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    releaseMemoryPool
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_releaseMemoryPool(
    JNIEnv* env, jclass, jlong memory_pool_id) {
  JNI_METHOD_START
  if (memory_pool_id == default_memory_pool_id) {
    return;
  }
  ReservationListenableMemoryPool* pool =
      reinterpret_cast<ReservationListenableMemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    return;
  }
  std::shared_ptr<ReserveFromJava> rm =
      std::dynamic_pointer_cast<ReserveFromJava>(pool->get_listener());
  if (rm == nullptr) {
    delete pool;
    return;
  }
  delete pool;
  env->DeleteGlobalRef(rm->GetJavaReservationListener());
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    bytesAllocated
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_bytesAllocated(
    JNIEnv* env, jclass, jlong memory_pool_id) {
  JNI_METHOD_START
  arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool instance not found. It may not exist or have been closed");
  }
  return pool->bytes_allocated();
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDatasetFactory
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDatasetFactory(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::DatasetFactory>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    inspectSchema
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_inspectSchema(
    JNIEnv* env, jobject, jlong dataset_factor_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factor_id);
  std::shared_ptr<arrow::Schema> schema = JniGetOrThrow(d->Inspect());
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createDataset
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createDataset(
    JNIEnv* env, jobject, jlong dataset_factory_id, jbyteArray schema_bytes) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factory_id);
  std::shared_ptr<arrow::Schema> schema;
  schema = JniGetOrThrow(FromSchemaByteArray(env, schema_bytes));
  std::shared_ptr<arrow::dataset::Dataset> dataset = JniGetOrThrow(d->Finish(schema));
  return CreateNativeRef(dataset);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataset(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::Dataset>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createScanner
 * Signature: (J[Ljava/lang/String;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createScanner(
    JNIEnv* env, jobject, jlong dataset_id, jobjectArray columns,
    jobject substrait_projection, jobject substrait_filter,
    jlong batch_size, jlong memory_pool_id) {
  JNI_METHOD_START
  arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      RetrieveNativeInstance<arrow::dataset::Dataset>(dataset_id);
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
      JniGetOrThrow(dataset->NewScan());
  JniAssertOkOrThrow(scanner_builder->Pool(pool));
  if (columns != nullptr) {
    std::vector<std::string> column_vector = ToStringVector(env, columns);
    JniAssertOkOrThrow(scanner_builder->Project(column_vector));
  }
  if (substrait_projection != nullptr) {
    std::shared_ptr<arrow::Buffer> buffer = LoadArrowBufferFromByteBuffer(env,
                                                            substrait_projection);
    std::vector<arrow::compute::Expression> project_exprs;
    std::vector<std::string> project_names;
    arrow::engine::BoundExpressions bounded_expression =
          JniGetOrThrow(arrow::engine::DeserializeExpressions(*buffer));
    for(arrow::engine::NamedExpression& named_expression :
                                        bounded_expression.named_expressions) {
      project_exprs.push_back(std::move(named_expression.expression));
      project_names.push_back(std::move(named_expression.name));
    }
    JniAssertOkOrThrow(scanner_builder->Project(std::move(project_exprs), std::move(project_names)));
  }
  if (substrait_filter != nullptr) {
    std::shared_ptr<arrow::Buffer> buffer = LoadArrowBufferFromByteBuffer(env,
                                                                substrait_filter);
    std::optional<arrow::compute::Expression> filter_expr = std::nullopt;
    arrow::engine::BoundExpressions bounded_expression =
          JniGetOrThrow(arrow::engine::DeserializeExpressions(*buffer));
    for(arrow::engine::NamedExpression& named_expression :
                                        bounded_expression.named_expressions) {
      filter_expr = named_expression.expression;
      if (named_expression.expression.type()->id() == arrow::Type::BOOL) {
        filter_expr = named_expression.expression;
      } else {
        JniThrow("There is no filter expression in the expression provided");
      }
    }
    if (filter_expr == std::nullopt) {
      JniThrow("The filter expression has not been provided");
    }
    JniAssertOkOrThrow(scanner_builder->Filter(*filter_expr));
  }
  JniAssertOkOrThrow(scanner_builder->BatchSize(batch_size));

  auto scanner = JniGetOrThrow(scanner_builder->Finish());
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      JniGetOrThrow(DisposableScannerAdaptor::Create(scanner));
  jlong id = CreateNativeRef(scanner_adaptor);
  return id;
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanner
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanner(
    JNIEnv* env, jobject, jlong scanner_id) {
  JNI_METHOD_START
  ReleaseNativeRef<DisposableScannerAdaptor>(scanner_id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getSchemaFromScanner
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_dataset_jni_JniWrapper_getSchemaFromScanner(JNIEnv* env, jobject,
                                                                  jlong scanner_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id)
          ->GetScanner()
          ->options()
          ->projected_schema;
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch(
    JNIEnv* env, jobject, jlong scanner_id, jlong struct_array) {
  JNI_METHOD_START
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id);

  std::shared_ptr<arrow::RecordBatch> record_batch =
      JniGetOrThrow(scanner_adaptor->Next());
  if (record_batch == nullptr) {
    return false;  // stream ended
  }
  std::vector<std::shared_ptr<arrow::Array>> offset_zeroed_arrays;
  for (int i = 0; i < record_batch->num_columns(); ++i) {
    // TODO: If the array has an offset then we need to de-offset the array
    // in order for it to be properly consumed on the Java end.
    // This forces a copy, it would be nice to avoid this if Java
    // could consume offset-arrays.  Perhaps at some point in the future
    // using the C data interface.  See ARROW-15275
    //
    // Generally a non-zero offset will occur whenever the scanner batch
    // size is smaller than the batch size of the underlying files.
    std::shared_ptr<arrow::Array> array = record_batch->column(i);
    if (array->offset() == 0) {
      offset_zeroed_arrays.push_back(array);
      continue;
    }
    std::shared_ptr<arrow::Array> offset_zeroed =
        JniGetOrThrow(arrow::Concatenate({array}));
    offset_zeroed_arrays.push_back(offset_zeroed);
  }

  std::shared_ptr<arrow::RecordBatch> offset_zeroed_batch = arrow::RecordBatch::Make(
      record_batch->schema(), record_batch->num_rows(), offset_zeroed_arrays);
  JniAssertOkOrThrow(
      arrow::dataset::jni::ExportRecordBatch(env, offset_zeroed_batch, struct_array));
  return true;
  JNI_METHOD_END(false)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::Buffer>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    ensureS3Finalized
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_ensureS3Finalized(
    JNIEnv* env, jobject) {
  JNI_METHOD_START
#ifdef ARROW_S3
  JniAssertOkOrThrow(arrow::fs::EnsureS3Finalized());
#endif
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeFileSystemDatasetFactory
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeFileSystemDatasetFactory__Ljava_lang_String_2I(
    JNIEnv* env, jobject, jstring uri, jint file_format_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemFactoryOptions options;
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      JniGetOrThrow(arrow::dataset::FileSystemDatasetFactory::Make(
          JStringToCString(env, uri), file_format, options));
  return CreateNativeRef(d);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeFileSystemDatasetFactory
 * Signature: ([Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeFileSystemDatasetFactory___3Ljava_lang_String_2I(
    JNIEnv* env, jobject, jobjectArray uris, jint file_format_id) {
  JNI_METHOD_START

  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemFactoryOptions options;

  std::vector<std::string> uri_vec = ToStringVector(env, uris);
  if (uri_vec.size() == 0) {
    JniThrow("No URIs provided.");
  }

  // If not all URIs, throw exception
  if (auto elem = std::find_if_not(uri_vec.begin(), uri_vec.end(), arrow::fs::internal::IsLikelyUri);
      elem != uri_vec.end()) {
    JniThrow("Unrecognized file type in URI: " + *elem);
  }

  std::vector<std::string> output_paths;
  std::string first_path;
  // We know that uri_vec isn't empty, from the conditional above
  auto fs = JniGetOrThrow(arrow::fs::FileSystemFromUri(uri_vec[0], &first_path));
  output_paths.push_back(first_path);

  std::transform(uri_vec.begin() + 1, uri_vec.end(), std::back_inserter(output_paths),
    [&](const auto& s) -> std::string {
    auto result = JniGetOrThrow(fs->PathFromUri(s));
    return std::move(result);
  });

  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      JniGetOrThrow(arrow::dataset::FileSystemDatasetFactory::Make(
        std::move(fs), std::move(output_paths), file_format, options));
  return CreateNativeRef(d);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    writeFromScannerToFile
 * Signature:
 * (JJJLjava/lang/String;[Ljava/lang/String;ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_writeFromScannerToFile(
    JNIEnv* env, jobject, jlong c_arrow_array_stream_address,
    jlong file_format_id, jstring uri, jobjectArray partition_columns,
    jint max_partitions, jstring base_name_template) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }

  auto* arrow_stream = reinterpret_cast<ArrowArrayStream*>(c_arrow_array_stream_address);
  std::shared_ptr<arrow::RecordBatchReader> reader =
      JniGetOrThrow(arrow::ImportRecordBatchReader(arrow_stream));
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
      arrow::dataset::ScannerBuilder::FromRecordBatchReader(reader);
  JniAssertOkOrThrow(scanner_builder->Pool(arrow::default_memory_pool()));
  auto scanner = JniGetOrThrow(scanner_builder->Finish());

  std::shared_ptr<arrow::Schema> schema = reader->schema();

  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemDatasetWriteOptions options;
  std::string output_path;
  auto filesystem = JniGetOrThrow(
      arrow::fs::FileSystemFromUri(JStringToCString(env, uri), &output_path));
  std::vector<std::string> partition_column_vector =
      ToStringVector(env, partition_columns);
  options.file_write_options = file_format->DefaultWriteOptions();
  options.filesystem = filesystem;
  options.base_dir = output_path;
  options.basename_template = JStringToCString(env, base_name_template);
  options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      SchemaFromColumnNames(schema, partition_column_vector).ValueOrDie());
  options.max_partitions = max_partitions;
  JniAssertOkOrThrow(arrow::dataset::FileSystemDataset::Write(options, scanner));
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_substrait_JniWrapper
 * Method:    executeSerializedPlan
 * Signature: (Ljava/lang/String;[Ljava/lang/String;J)V
 */
JNIEXPORT void JNICALL
    Java_org_apache_arrow_dataset_substrait_JniWrapper_executeSerializedPlan__Ljava_lang_String_2_3Ljava_lang_String_2J (
    JNIEnv* env, jobject, jstring plan, jobjectArray table_to_memory_address_input,
    jlong memory_address_output) {
  JNI_METHOD_START
  // get mapping of table name to memory address
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> map_table_to_reader =
    LoadNamedTables(env, table_to_memory_address_input);
  // create table provider
  arrow::engine::NamedTableProvider table_provider =
    [&map_table_to_reader](const std::vector<std::string>& names, const arrow::Schema&) {
    std::shared_ptr<arrow::Table> output_table = GetTableByName(names, map_table_to_reader);
    std::shared_ptr<arrow::acero::ExecNodeOptions> options =
      std::make_shared<arrow::acero::TableSourceNodeOptions>(std::move(output_table));
    return arrow::acero::Declaration("table_source", {}, options, "java_source");
  };
  arrow::engine::ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  // execute plan
  std::shared_ptr<arrow::Buffer> buffer = JniGetOrThrow(arrow::engine::SerializeJsonPlan(
    JStringToCString(env, plan)));
  std::shared_ptr<arrow::RecordBatchReader> reader_out =
    JniGetOrThrow(arrow::engine::ExecuteSerializedPlan(*buffer, nullptr, nullptr, conversion_options));
  auto* arrow_stream_out = reinterpret_cast<ArrowArrayStream*>(memory_address_output);
  JniAssertOkOrThrow(arrow::ExportRecordBatchReader(reader_out, arrow_stream_out));
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_substrait_JniWrapper
 * Method:    executeSerializedPlan
 * Signature: (Ljava/nio/ByteBuffer;[Ljava/lang/String;J)V
 */
JNIEXPORT void JNICALL
    Java_org_apache_arrow_dataset_substrait_JniWrapper_executeSerializedPlan__Ljava_nio_ByteBuffer_2_3Ljava_lang_String_2J (
    JNIEnv* env, jobject, jobject plan, jobjectArray table_to_memory_address_input,
    jlong memory_address_output) {
  JNI_METHOD_START
  // get mapping of table name to memory address
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> map_table_to_reader =
    LoadNamedTables(env, table_to_memory_address_input);
  // create table provider
  arrow::engine::NamedTableProvider table_provider =
    [&map_table_to_reader](const std::vector<std::string>& names, const arrow::Schema&) {
    std::shared_ptr<arrow::Table> output_table = GetTableByName(names, map_table_to_reader);
    std::shared_ptr<arrow::acero::ExecNodeOptions> options =
      std::make_shared<arrow::acero::TableSourceNodeOptions>(std::move(output_table));
    return arrow::acero::Declaration("table_source", {}, options, "java_source");
  };
  arrow::engine::ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  // mapping arrow::Buffer
  std::shared_ptr<arrow::Buffer> buffer = LoadArrowBufferFromByteBuffer(env, plan);
  // execute plan
  std::shared_ptr<arrow::RecordBatchReader> reader_out =
    JniGetOrThrow(arrow::engine::ExecuteSerializedPlan(*buffer, nullptr, nullptr, conversion_options));
  auto* arrow_stream_out = reinterpret_cast<ArrowArrayStream*>(memory_address_output);
  JniAssertOkOrThrow(arrow::ExportRecordBatchReader(reader_out, arrow_stream_out));
  JNI_METHOD_END()
}
