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

#include <arrow/buffer.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <jni.h>
#include <iostream>
#include <string>

#include "jni/parquet/concurrent_map.h"
#include "jni/parquet/parquet_reader.h"
#include "jni/parquet/parquet_writer.h"

static jclass arrow_record_batch_builder_class;
static jmethodID arrow_record_batch_builder_constructor;

static jclass arrow_field_node_builder_class;
static jmethodID arrow_field_node_builder_constructor;

static jclass arrowbuf_builder_class;
static jmethodID arrowbuf_builder_constructor;

using arrow::jni::ConcurrentMap;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

static jclass io_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;

static jint JNI_VERSION = JNI_VERSION_1_8;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  if (global_class == nullptr) {
    std::string error_message =
        "Unable to createGlobalClassReference for" + std::string(class_name);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }

  return ret;
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  std::vector<char> buffer(clen);
  env->GetStringUTFRegion(string, 0, jlen, buffer.data());
  return std::string(buffer.data(), clen);
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  arrow_record_batch_builder_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/adapter/parquet/ArrowRecordBatchBuilder;");
  arrow_record_batch_builder_constructor =
      GetMethodID(env, arrow_record_batch_builder_class, "<init>",
                  "(I[Lorg/apache/arrow/adapter/parquet/ArrowFieldNodeBuilder;"
                  "[Lorg/apache/arrow/adapter/parquet/ArrowBufBuilder;)V");

  arrow_field_node_builder_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/adapter/parquet/ArrowFieldNodeBuilder;");
  arrow_field_node_builder_constructor =
      GetMethodID(env, arrow_field_node_builder_class, "<init>", "(II)V");

  arrowbuf_builder_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/adapter/parquet/ArrowBufBuilder;");
  arrowbuf_builder_constructor =
      GetMethodID(env, arrowbuf_builder_class, "<init>", "(JJIJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(arrow_field_node_builder_class);
  env->DeleteGlobalRef(arrowbuf_builder_class);
  env->DeleteGlobalRef(arrow_record_batch_builder_class);

  buffer_holder_.Clear();
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeOpenParquetReader(
    JNIEnv* env, jobject obj, jstring path) {
  std::string cpath = JStringToCString(env, path);

  jni::parquet::ParquetReader* reader = new jni::parquet::ParquetReader(cpath);
  return (int64_t)reader;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeInitParquetReader(
    JNIEnv* env, jobject obj, jlong reader_ptr, jintArray column_indices,
    jintArray row_group_indices, jlong batch_size, jboolean useHdfs3) {
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;

  ::arrow::Status msg;

  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);

  int row_group_indices_len = env->GetArrayLength(row_group_indices);
  if (row_group_indices_len == 0) {
    std::vector<int> _row_group_indices = {};
    msg = reader->initialize(_row_group_indices, _column_indices, batch_size, useHdfs3);
  } else {
    jint* row_group_indices_ptr = env->GetIntArrayElements(row_group_indices, 0);
    std::vector<int> _row_group_indices(row_group_indices_ptr,
                                        row_group_indices_ptr + row_group_indices_len);
    msg = reader->initialize(_row_group_indices, _column_indices, batch_size, useHdfs3);
    env->ReleaseIntArrayElements(row_group_indices, row_group_indices_ptr, JNI_ABORT);
  }
  if (!msg.ok()) {
    std::string error_message =
        "nativeInitParquetReader: failed to initialize, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeInitParquetReader2(
    JNIEnv* env, jobject obj, jlong reader_ptr, jintArray column_indices, jlong start_pos,
    jlong end_pos, jlong batch_size, jboolean useHdfs3) {
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;

  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);

  ::arrow::Status msg;
  msg = reader->initialize(_column_indices, start_pos, end_pos, batch_size, useHdfs3);
  if (!msg.ok()) {
    std::string error_message =
        "nativeInitParquetReader: failed to initialize, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeCloseParquetReader(
    JNIEnv* env, jobject obj, jlong reader_ptr) {
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;
  delete reader;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeReadNext(
    JNIEnv* env, jobject obj, jlong reader_ptr) {
  std::shared_ptr<::arrow::RecordBatch> record_batch;
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;
  arrow::Status status = reader->readNext(&record_batch);

  if (!status.ok() || !record_batch) {
    return nullptr;
  }

  auto schema = reader->schema();

  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), arrow_field_node_builder_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(arrow_field_node_builder_class,
                                   arrow_field_node_builder_constructor, column->length(),
                                   column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray arrowbuf_builder_array =
      env->NewObjectArray(buffers.size(), arrowbuf_builder_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    jobject arrowBufBuilder =
        env->NewObject(arrowbuf_builder_class, arrowbuf_builder_constructor,
                       buffer_holder_.Insert(buffer), buffer->data(), (int)buffer->size(),
                       buffer->capacity());
    env->SetObjectArrayElement(arrowbuf_builder_array, j, arrowBufBuilder);
  }

  // create RecordBatch
  jobject arrowRecordBatchBuilder = env->NewObject(
      arrow_record_batch_builder_class, arrow_record_batch_builder_constructor,
      record_batch->num_rows(), field_array, arrowbuf_builder_array);
  return arrowRecordBatchBuilder;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeGetSchema(
    JNIEnv* env, jobject obj, jlong reader_ptr) {
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;
  std::shared_ptr<::arrow::Schema> schema = reader->schema();
  std::shared_ptr<arrow::Buffer> out;
  arrow::Status status =
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    return nullptr;
  }

  jbyteArray ret = env->NewByteArray(out->size());
  auto src = reinterpret_cast<const jbyte*>(out->data());
  env->SetByteArrayRegion(ret, 0, out->size(), src);
  return ret;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_AdaptorReferenceManager_nativeRelease(
    JNIEnv* env, jobject this_obj, jlong id) {
  buffer_holder_.Erase(id);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeOpenParquetWriter(
    JNIEnv* env, jobject obj, jstring path, jbyteArray schemaBytes) {
  int schemaBytes_len = env->GetArrayLength(schemaBytes);
  jbyte* schemaBytes_data = env->GetByteArrayElements(schemaBytes, 0);

  auto serialized_schema =
      std::make_shared<arrow::Buffer>((uint8_t*)schemaBytes_data, schemaBytes_len);
  arrow::ipc::DictionaryMemo in_memo;
  std::shared_ptr<arrow::Schema> schema;
  arrow::io::BufferReader buf_reader(serialized_schema);

  arrow::Status msg = arrow::ipc::ReadSchema(&buf_reader, &in_memo, &schema);
  if (!msg.ok()) {
    std::string error_message =
        "nativeOpenParquetWriter: failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::string cpath = JStringToCString(env, path);
  jni::parquet::ParquetWriter* writer = new jni::parquet::ParquetWriter(cpath, schema);

  env->ReleaseByteArrayElements(schemaBytes, schemaBytes_data, JNI_ABORT);
  return (int64_t)writer;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeInitParquetWriter(
    JNIEnv* env, jobject obj, jlong writer_ptr, jboolean useHdfs3, jint rep) {
  jni::parquet::ParquetWriter* writer = (jni::parquet::ParquetWriter*)writer_ptr;
  ::arrow::Status msg = writer->initialize(useHdfs3, rep);
  if (!msg.ok()) {
    std::string error_message =
        "nativeInitParquetWriter: failed to initialize, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeCloseParquetWriter(
    JNIEnv* env, jobject obj, jlong writer_ptr) {
  jni::parquet::ParquetWriter* writer = (jni::parquet::ParquetWriter*)writer_ptr;
  arrow::Status msg = writer->flush();
  if (!msg.ok()) {
    std::string error_message =
        "nativeCloseParquetWriter: failed to flush, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  delete writer;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeWriteNext(
    JNIEnv* env, jobject obj, jlong writer_ptr, jint numRows, jlongArray bufAddrs,
    jlongArray bufSizes) {
  int in_bufs_len = env->GetArrayLength(bufAddrs);
  if (in_bufs_len != env->GetArrayLength(bufSizes)) {
    std::string error_message =
        "nativeWriteNext: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(bufAddrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(bufSizes, 0);

  jni::parquet::ParquetWriter* writer = (jni::parquet::ParquetWriter*)writer_ptr;
  arrow::Status msg = writer->writeNext(numRows, in_buf_addrs, in_buf_sizes, in_bufs_len);

  if (!msg.ok()) {
    std::string error_message = "nativeWriteNext: failed, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(bufAddrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(bufSizes, in_buf_sizes, JNI_ABORT);
}

#ifdef __cplusplus
}
#endif
