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
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <jni.h>
#include <iostream>
#include <string>

#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
#include "jni/parquet/adapter.h"

static jint JNI_VERSION = JNI_VERSION_1_8;

using FileSystem = arrow::fs::FileSystem;
using ParquetFileReader = jni::parquet::adapters::ParquetFileReader;
using ParquetFileWriter = jni::parquet::adapters::ParquetFileWriter;

static arrow::jni::ConcurrentMap<std::shared_ptr<ParquetFileReader>> reader_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ParquetFileWriter>> writer_holder_;

std::shared_ptr<ParquetFileReader> GetFileReader(JNIEnv* env, jlong id) {
  auto reader = reader_holder_.Lookup(id);
  if (!reader) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return reader;
}

std::shared_ptr<ParquetFileWriter> GetFileWriter(JNIEnv* env, jlong id) {
  auto writer = writer_holder_.Lookup(id);
  if (!writer) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return writer;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  LoadExceptionClassReferences(env);
  LoadRecordBatchClassReferences(env);

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  UnloadExceptionClassReferences(env);
  UnloadRecordBatchClassReferences(env);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeOpenParquetReader(
    JNIEnv* env, jobject obj, jstring path, jlong batch_size) {
  arrow::Status status;
  std::string cpath = JStringToCString(env, path);

  std::shared_ptr<FileSystem> fs;
  std::string file_name;
  status = arrow::fs::FileSystemFromUri(cpath, &fs, &file_name);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetReader: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::io::RandomAccessFile> file;
  status = fs->OpenInputFile(file_name).Value(&file);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetReader: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  parquet::ArrowReaderProperties properties(true);
  properties.set_batch_size(batch_size);

  std::unique_ptr<ParquetFileReader> reader;
  status =
      ParquetFileReader::Open(file, arrow::default_memory_pool(), properties, &reader);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetReader: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return reader_holder_.Insert(std::shared_ptr<ParquetFileReader>(reader.release()));
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeInitParquetReader(
    JNIEnv* env, jobject obj, jlong id, jintArray column_indices,
    jintArray row_group_indices) {
  // Prepare column_indices and row_group_indices from java array.
  bool column_indices_need_release = false;
  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);
  column_indices_need_release = true;

  bool row_group_indices_need_release = false;
  std::vector<int> _row_group_indices = {};
  jint* row_group_indices_ptr;
  int row_group_indices_len = env->GetArrayLength(row_group_indices);
  if (row_group_indices_len != 0) {
    row_group_indices_ptr = env->GetIntArrayElements(row_group_indices, 0);
    std::vector<int> rg_indices_tmp(row_group_indices_ptr,
                                    row_group_indices_ptr + row_group_indices_len);
    _row_group_indices = rg_indices_tmp;
    row_group_indices_need_release = true;
  }

  // Call ParquetFileReader init func.
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  status = reader->InitRecordBatchReader(_column_indices, _row_group_indices);
  if (!status.ok()) {
    std::string error_message =
        "nativeInitParquetReader: failed to Initialize, err msg is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (column_indices_need_release) {
    env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
  }
  if (row_group_indices_need_release) {
    env->ReleaseIntArrayElements(row_group_indices, row_group_indices_ptr, JNI_ABORT);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeInitParquetReader2(
    JNIEnv* env, jobject obj, jlong id, jintArray column_indices, jlong start_pos,
    jlong end_pos) {
  // Prepare column_indices and row_group_indices from java array.
  bool column_indices_need_release = false;
  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr,
                                   column_indices_ptr + column_indices_len);
  column_indices_need_release = true;

  // Call ParquetFileReader init func.
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  status = reader->InitRecordBatchReader(_column_indices, start_pos, end_pos);
  if (!status.ok()) {
    std::string error_message =
        "nativeInitParquetReader2: failed to Initialize, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (column_indices_need_release) {
    env->ReleaseIntArrayElements(column_indices, column_indices_ptr, JNI_ABORT);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeCloseParquetReader(
    JNIEnv* env, jobject obj, jlong id) {
  reader_holder_.Erase(id);
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeReadNext(JNIEnv* env,
                                                                             jobject obj,
                                                                             jlong id) {
  arrow::Status status;
  auto reader = GetFileReader(env, id);

  std::shared_ptr<arrow::RecordBatch> record_batch;
  status = reader->ReadNext(&record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeReadNext: failed to read next batch, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  if (record_batch == nullptr) {
    return nullptr;
  }
  std::shared_ptr<arrow::Schema> schema;
  status = reader->ReadSchema(&schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeReadNext: failed to read schema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return MakeRecordBatchBuilder(env, schema, record_batch);
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetReaderJniWrapper_nativeGetSchema(JNIEnv* env,
                                                                              jobject obj,
                                                                              jlong id) {
  arrow::Status status;
  auto reader = GetFileReader(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = reader->ReadSchema(&schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeGetSchema: failed to read schema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jbyteArray ret = ToSchemaByteArray(env, schema);
  if (ret == nullptr) {
    std::string error_message = "nativeGetSchema: failed to convert schema to byte array";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return ret;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_common_AdaptorReferenceManager_nativeRelease(
    JNIEnv* env, jobject this_obj, jlong id) {
  ReleaseBuffer(id);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeOpenParquetWriter(
    JNIEnv* env, jobject obj, jstring path, jbyteArray schemaBytes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  status = FromSchemaByteArray(env, schemaBytes, &schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeOpenParquetWriter: failed to readSchema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::string cpath = JStringToCString(env, path);

  std::shared_ptr<FileSystem> fs;
  std::string file_name;
  status = arrow::fs::FileSystemFromUri(cpath, &fs, &file_name);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetWriter: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::io::OutputStream> sink;
  status = fs->OpenOutputStream(file_name).Value(&sink);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetWriter: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::unique_ptr<ParquetFileWriter> writer;
  status = ParquetFileWriter::Open(sink, arrow::default_memory_pool(), schema, &writer);
  if (!status.ok()) {
    std::string error_message = "nativeOpenParquetWriter: " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return writer_holder_.Insert(std::shared_ptr<ParquetFileWriter>(writer.release()));
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeCloseParquetWriter(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  auto writer = GetFileWriter(env, id);
  status = writer->Flush();
  if (!status.ok()) {
    std::string error_message =
        "nativeCloseParquetWriter: failed to Flush, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  writer_holder_.Erase(id);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adapter_parquet_ParquetWriterJniWrapper_nativeWriteNext(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray bufAddrs,
    jlongArray bufSizes) {
  // convert input data to record batch
  int in_bufs_len = env->GetArrayLength(bufAddrs);
  if (in_bufs_len != env->GetArrayLength(bufSizes)) {
    std::string error_message =
        "nativeWriteNext: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(bufAddrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(bufSizes, 0);

  arrow::Status status;
  auto writer = GetFileWriter(env, id);

  std::shared_ptr<arrow::Schema> schema;
  status = writer->GetSchema(&schema);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to read schema, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;
  status = MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len,
                           &record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to get record batch, err is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  status = writer->WriteNext(record_batch);
  if (!status.ok()) {
    std::string error_message =
        "nativeWriteNext: failed to write next batch, err msg is " + status.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(bufAddrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(bufSizes, in_buf_sizes, JNI_ABORT);
}

#ifdef __cplusplus
}
#endif
