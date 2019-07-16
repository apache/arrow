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

#include <arrow/adapters/orc/adapter.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/logging.h>
#include <cassert>
#include <iostream>
#include <string>

#include "org_apache_arrow_adapter_orc_OrcMemoryJniWrapper.h"
#include "org_apache_arrow_adapter_orc_OrcReaderJniWrapper.h"
#include "org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper.h"

#include "./concurrent_map.h"

using ORCFileReader = arrow::adapters::orc::ORCFileReader;
using RecordBatchReader = arrow::RecordBatchReader;

static jclass io_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;

static jclass orc_field_node_class;
static jmethodID orc_field_node_constructor;

static jclass orc_memory_class;
static jmethodID orc_memory_constructor;

static jclass record_batch_class;
static jmethodID record_batch_constructor;

static jint JNI_VERSION = JNI_VERSION_1_6;

using arrow::jni::ConcurrentMap;

static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;
static ConcurrentMap<std::shared_ptr<RecordBatchReader>> orc_stripe_reader_holder_;
static ConcurrentMap<std::shared_ptr<ORCFileReader>> orc_reader_holder_;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
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

std::shared_ptr<ORCFileReader> GetFileReader(JNIEnv* env, jlong id) {
  auto reader = orc_reader_holder_.Lookup(id);
  if (!reader) {
    std::string error_message = "invalid reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }

  return reader;
}

std::shared_ptr<RecordBatchReader> GetStripeReader(JNIEnv* env, jlong id) {
  auto reader = orc_stripe_reader_holder_.Lookup(id);
  if (!reader) {
    std::string error_message = "invalid stripe reader id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }

  return reader;
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

  orc_field_node_class =
      CreateGlobalClassReference(env, "Lorg/apache/arrow/adapter/orc/OrcFieldNode;");
  orc_field_node_constructor = GetMethodID(env, orc_field_node_class, "<init>", "(II)V");

  orc_memory_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/adapter/orc/OrcMemoryJniWrapper;");
  orc_memory_constructor = GetMethodID(env, orc_memory_class, "<init>", "(JJJJ)V");

  record_batch_class =
      CreateGlobalClassReference(env, "Lorg/apache/arrow/adapter/orc/OrcRecordBatch;");
  record_batch_constructor =
      GetMethodID(env, record_batch_class, "<init>",
                  "(I[Lorg/apache/arrow/adapter/orc/OrcFieldNode;"
                  "[Lorg/apache/arrow/adapter/orc/OrcMemoryJniWrapper;)V");

  env->ExceptionDescribe();

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(orc_field_node_class);
  env->DeleteGlobalRef(orc_memory_class);
  env->DeleteGlobalRef(record_batch_class);

  buffer_holder_.Clear();
  orc_stripe_reader_holder_.Clear();
  orc_reader_holder_.Clear();
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_open(
    JNIEnv* env, jobject this_obj, jstring file_path) {
  std::shared_ptr<arrow::io::ReadableFile> in_file;

  std::string path = JStringToCString(env, file_path);

  arrow::Status ret;
  if (path.find("hdfs://") == 0) {
    env->ThrowNew(io_exception_class, "hdfs path not supported yet.");
  } else {
    ret = arrow::io::ReadableFile::Open(path, &in_file);
  }

  if (ret.ok()) {
    std::unique_ptr<ORCFileReader> reader;

    ret = ORCFileReader::Open(
        std::static_pointer_cast<arrow::io::RandomAccessFile>(in_file),
        arrow::default_memory_pool(), &reader);

    if (!ret.ok()) {
      env->ThrowNew(io_exception_class, std::string("Failed open file" + path).c_str());
    }

    return orc_reader_holder_.Insert(std::shared_ptr<ORCFileReader>(reader.release()));
  }

  return static_cast<jlong>(ret.code()) * -1;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_close(
    JNIEnv* env, jobject this_obj, jlong id) {
  orc_reader_holder_.Erase(id);
}

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_seek(
    JNIEnv* env, jobject this_obj, jlong id, jint row_number) {
  auto reader = GetFileReader(env, id);
  return reader->Seek(row_number).ok();
}

JNIEXPORT jint JNICALL
Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_getNumberOfStripes(JNIEnv* env,
                                                                         jobject this_obj,
                                                                         jlong id) {
  auto reader = GetFileReader(env, id);
  return reader->NumberOfStripes();
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_nextStripeReader(JNIEnv* env,
                                                                       jobject this_obj,
                                                                       jlong id,
                                                                       jlong batch_size) {
  auto reader = GetFileReader(env, id);

  std::shared_ptr<RecordBatchReader> stripe_reader;
  auto status = reader->NextStripeReader(batch_size, &stripe_reader);

  if (!status.ok()) {
    return static_cast<jlong>(status.code()) * -1;
  }

  if (!stripe_reader) {
    return static_cast<jlong>(arrow::StatusCode::Invalid) * -1;
  }

  return orc_stripe_reader_holder_.Insert(stripe_reader);
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_getSchema(JNIEnv* env,
                                                                      jclass this_cls,
                                                                      jlong id) {
  auto stripe_reader = GetStripeReader(env, id);

  auto schema = stripe_reader->schema();

  std::shared_ptr<arrow::Buffer> out;
  auto status =
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    return nullptr;
  }

  jbyteArray ret = env->NewByteArray(out->size());
  auto src = reinterpret_cast<const jbyte*>(out->data());
  env->SetByteArrayRegion(ret, 0, out->size(), src);
  return ret;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_next(JNIEnv* env,
                                                                 jclass this_cls,
                                                                 jlong id) {
  auto stripe_reader = GetStripeReader(env, id);

  std::shared_ptr<arrow::RecordBatch> record_batch;
  auto status = stripe_reader->ReadNext(&record_batch);
  if (!status.ok() || !record_batch) {
    return nullptr;
  }

  auto schema = stripe_reader->schema();

  // TODO: ARROW-4714 Ensure JVM has sufficient capacity to create local references
  // create OrcFieldNode[]
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), orc_field_node_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(orc_field_node_class, orc_field_node_constructor,
                                   column->length(), column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  // create OrcMemoryJniWrapper[]
  jobjectArray memory_array =
      env->NewObjectArray(buffers.size(), orc_memory_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    jobject memory = env->NewObject(orc_memory_class, orc_memory_constructor,
                                    buffer_holder_.Insert(buffer), buffer->data(),
                                    buffer->size(), buffer->capacity());
    env->SetObjectArrayElement(memory_array, j, memory);
  }

  // create OrcRecordBatch
  jobject ret = env->NewObject(record_batch_class, record_batch_constructor,
                               record_batch->num_rows(), field_array, memory_array);

  return ret;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_close(
    JNIEnv* env, jclass this_cls, jlong id) {
  orc_stripe_reader_holder_.Erase(id);
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcMemoryJniWrapper_release(
    JNIEnv* env, jobject this_obj, jlong id) {
  buffer_holder_.Erase(id);
}

#ifdef __cplusplus
}
#endif
