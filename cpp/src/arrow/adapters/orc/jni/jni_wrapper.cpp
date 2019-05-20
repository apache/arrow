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
#include <string>

#include "org_apache_arrow_adapter_orc_OrcMemoryJniWrapper.h"
#include "org_apache_arrow_adapter_orc_OrcReaderJniWrapper.h"
#include "org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper.h"

using ORCFileReader = arrow::adapters::orc::ORCFileReader;
using RecordBatchReader = arrow::RecordBatchReader;

void ThrowJavaIOException(JNIEnv* env, const std::exception& e) {
  jclass ioExceptionClass = env->FindClass("java/io/IOException");
  if (ioExceptionClass != nullptr) {
    if (env->ThrowNew(ioExceptionClass, e.what())) {
      // Failed to new IOException. This means another error has occurred in Java
      // We just propagate this error to caller by doing nothing.
      ARROW_LOG(ERROR) << "Error occurred when throwing IOException";
    }
  } else {
    ARROW_LOG(ERROR) << "Error occurred when getting IOException class";
  }
}

void ThrowJavaException(JNIEnv* env, const std::string& message) {
  jclass exception = env->FindClass("java/lang/Exception");
  if (exception != nullptr) {
    env->ThrowNew(exception, message.c_str());
  } else {
    throw std::runtime_error("Can't find java/lang/Exception class");
  }
}

jfieldID GetFieldId(JNIEnv* env, jclass this_class, const std::string& sig) {
  jfieldID ret = env->GetFieldID(this_class, sig.c_str(), "J");
  if (ret == nullptr) {
    ThrowJavaException(env, "Unable to get java class field: " + sig);
  }

  return ret;
}

std::unique_ptr<ORCFileReader>* GetNativeReader(JNIEnv* env, jobject this_obj) {
  jlong reader = env->GetLongField(
      this_obj, GetFieldId(env, env->GetObjectClass(this_obj), "nativeReaderAddress"));
  return reinterpret_cast<std::unique_ptr<ORCFileReader>*>(reader);
}

std::shared_ptr<RecordBatchReader>* GetStripeReader(JNIEnv* env, jobject this_obj) {
  jlong reader = env->GetLongField(
      this_obj,
      GetFieldId(env, env->GetObjectClass(this_obj), "nativeStripeReaderAddress"));
  return reinterpret_cast<std::shared_ptr<RecordBatchReader>*>(reader);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_open(
    JNIEnv* env, jobject this_obj, jstring file_path) {
  std::shared_ptr<arrow::io::ReadableFile> in_file;
  const char* str = env->GetStringUTFChars(file_path, nullptr);
  std::string path(str);
  env->ReleaseStringUTFChars(file_path, str);

  arrow::Status ret;
  if (path.find("hdfs://") == 0) {
    return false;
  } else {
    ret = arrow::io::ReadableFile::Open(path, &in_file);
  }

  if (ret.ok()) {
    auto reader = new std::unique_ptr<ORCFileReader>();

    ret = ORCFileReader::Open(
        std::static_pointer_cast<arrow::io::RandomAccessFile>(in_file),
        arrow::default_memory_pool(), reader);

    if (!ret.ok()) {
      delete reader;
      ThrowJavaIOException(env, std::invalid_argument("Failed open file" + path));
    }

    env->SetLongField(
        this_obj, GetFieldId(env, env->GetObjectClass(this_obj), "nativeReaderAddress"),
        reinterpret_cast<long>(reader));
  }

  return ret.ok();
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_close(
    JNIEnv* env, jobject this_obj) {
  delete GetNativeReader(env, this_obj);
}

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_seek(
    JNIEnv* env, jobject this_obj, jint row_number) {
  auto reader = GetNativeReader(env, this_obj);
  return (*reader)->Seek(row_number).ok();
}

JNIEXPORT jint JNICALL
Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_getNumberOfStripes(
    JNIEnv* env, jobject this_obj) {
  auto reader = GetNativeReader(env, this_obj);
  return (*reader)->NumberOfStripes();
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_nextStripeReader(JNIEnv* env,
                                                                       jobject this_obj,
                                                                       jlong batch_size) {
  auto reader = GetNativeReader(env, this_obj);
  auto stripe_reader = new std::shared_ptr<RecordBatchReader>();
  auto status = (*reader)->NextStripeReader(batch_size, stripe_reader);

  jobject ret = nullptr;
  if (!status.ok()) {
    delete stripe_reader;
    return ret;
  }

  jclass cls = env->FindClass("/org/apache/arrow/adapter/orc/OrcStripeReaderJniWrapper");
  ret = env->AllocObject(cls);

  env->SetLongField(ret, GetFieldId(env, cls, "nativeStripeReaderAddress"),
                    reinterpret_cast<long>(stripe_reader));

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_getSchema(JNIEnv* env,
                                                                      jobject this_obj) {
  auto stripe_reader = GetStripeReader(env, this_obj);
  auto schema = (*stripe_reader)->schema();

  std::shared_ptr<arrow::Buffer> out;
  auto status = arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    return nullptr;
  }

  jbyteArray ret = env->NewByteArray(out->size());
  memcpy(env->GetByteArrayElements(ret, nullptr), out->data(), out->size());
  return ret;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_next(JNIEnv* env,
                                                                 jobject this_obj) {
  auto stripe_reader = GetStripeReader(env, this_obj);
  auto record_batch = new std::shared_ptr<arrow::RecordBatch>();
  auto status = (*stripe_reader)->ReadNext(record_batch);
  if (!status.ok()) {
    delete record_batch;
    return nullptr;
  }

  auto schema = (*stripe_reader)->schema();

  // create OrcFieldNode[]
  jclass field_class = env->FindClass("/org/apache/arrow/adapter/orc/OrcFieldNode");
  jmethodID field_constructor = env->GetMethodID(field_class, "<init>", "(II)V");
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), field_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = (*record_batch)->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(field_class, field_constructor, column->length(),
                                   column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  // create OrcMemoryJniWrapper[]
  jclass memory_class =
      env->FindClass("/org/apache/arrow/adapter/orc/OrcMemoryJniWrapper");
  jmethodID memory_constructor = env->GetMethodID(memory_class, "<init>", "(JJJJ)V");
  jobjectArray memory_array = env->NewObjectArray(buffers.size(), memory_class, nullptr);

  for (int j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    jobject memory = env->NewObject(memory_class, memory_constructor, &buffer,
                                    buffer->data(), buffer->size(), buffer->capacity());
    env->SetObjectArrayElement(memory_array, j, memory);
  }

  // create OrcRecordBatch
  jclass ret_cls = env->FindClass("/org/apache/arrow/adapter/orc/OrcRecordBatch");
  jmethodID ret_constructor =
      env->GetMethodID(ret_cls, "<init>",
                       "(I[L/org/apache/arrow/adapter/orc/OrcFieldNode;"
                       "[L/org/apache/arrow/adapter/orc/OrcMemoryJniWrapper;)V");
  jobject ret = env->NewObject(ret_cls, ret_constructor, (*record_batch)->num_rows(),
                               field_array, memory_array);

  return ret;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcStripeReaderJniWrapper_close(
    JNIEnv* env, jobject this_obj) {
  delete GetStripeReader(env, this_obj);
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_orc_OrcMemoryJniWrapper_release(
    JNIEnv* env, jobject this_obj, jlong address) {
  delete reinterpret_cast<std::shared_ptr<arrow::Buffer>*>(address);
}

#ifdef __cplusplus
}
#endif
