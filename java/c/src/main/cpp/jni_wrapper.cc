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

#include <jni.h>

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "./abi.h"
#include "org_apache_arrow_c_jni_JniWrapper.h"

namespace {

jclass kObjectClass;
jclass kRuntimeExceptionClass;
jclass kPrivateDataClass;
jclass kCDataExceptionClass;
jclass kStreamPrivateDataClass;

jfieldID kPrivateDataLastErrorField;

jmethodID kObjectToStringMethod;
jmethodID kPrivateDataCloseMethod;
jmethodID kPrivateDataGetNextMethod;
jmethodID kPrivateDataGetSchemaMethod;
jmethodID kCDataExceptionConstructor;

jint JNI_VERSION = JNI_VERSION_1_6;

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : std::runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

void JniThrow(std::string message) { ThrowPendingException(message); }

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  if (!local_class) {
    std::string message = "Could not find class ";
    message += class_name;
    ThrowPendingException(message);
  }
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  if (!global_class) {
    std::string message = "Could not create global reference to class ";
    message += class_name;
    ThrowPendingException(message);
  }
  env->DeleteLocalRef(local_class);
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " with signature " + std::string(sig);
    ThrowPendingException(error_message);
  }
  return ret;
}

jfieldID GetFieldID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jfieldID fieldId = env->GetFieldID(this_class, name, sig);
  if (fieldId == nullptr) {
    std::string error_message = "Unable to find field " + std::string(name) +
                                " with signature " + std::string(sig);
    ThrowPendingException(error_message);
  }
  return fieldId;
}

class InnerPrivateData {
 public:
  InnerPrivateData(JavaVM* vm, jobject private_data)
      : vm_(vm), j_private_data_(private_data) {}

  JavaVM* vm_;
  jobject j_private_data_;
  // Only for ArrowArrayStream
  std::string last_error_;
};

class JNIEnvGuard {
 public:
  explicit JNIEnvGuard(JavaVM* vm) : vm_(vm), should_detach_(false) {
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
  bool should_detach_;
  JavaVM* vm_;
  JNIEnv* env_;
};

template <typename T>
void release_exported(T* base) {
  // This should not be called on already released structure
  assert(base->release != nullptr);

  // Release children
  for (int64_t i = 0; i < base->n_children; ++i) {
    T* child = base->children[i];
    if (child->release != nullptr) {
      child->release(child);
      assert(child->release == nullptr);
    }
  }

  // Release dictionary
  T* dict = base->dictionary;
  if (dict != nullptr && dict->release != nullptr) {
    dict->release(dict);
    assert(dict->release == nullptr);
  }

  // Release all data directly owned by the struct
  InnerPrivateData* private_data =
      reinterpret_cast<InnerPrivateData*>(base->private_data);

  // It is possible for the JVM to be shut down when this is called;
  // guard against that.  Example: Python code using JPype may shut
  // down the JVM before releasing the stream.
  try {
    JNIEnvGuard guard(private_data->vm_);
    JNIEnv* env = guard.env();

    env->CallObjectMethod(private_data->j_private_data_, kPrivateDataCloseMethod);
    if (env->ExceptionCheck()) {
      // Can't signal this to caller, so log and then try to free things
      // as best we can
      env->ExceptionDescribe();
      env->ExceptionClear();
    }
    env->DeleteGlobalRef(private_data->j_private_data_);
  } catch (const JniPendingException& e) {
    std::cerr << "WARNING: Failed to release Java C Data resource: " << e.what()
              << std::endl;
  }
  delete private_data;
  base->private_data = nullptr;

  // Mark released
  base->release = nullptr;
}

// Attempt to copy the JVM-side lastError to the C++ side
void TryCopyLastError(JNIEnv* env, InnerPrivateData* private_data) {
  jobject error_data =
      env->GetObjectField(private_data->j_private_data_, kPrivateDataLastErrorField);
  if (!error_data) {
    private_data->last_error_.clear();
    return;
  }

  auto arr = reinterpret_cast<jbyteArray>(error_data);
  jbyte* error_bytes = env->GetByteArrayElements(arr, nullptr);
  if (!error_bytes) {
    private_data->last_error_.clear();
    return;
  }

  char* error_str = reinterpret_cast<char*>(error_bytes);
  private_data->last_error_ = std::string(error_str, std::strlen(error_str));

  env->ReleaseByteArrayElements(arr, error_bytes, JNI_ABORT);
}

// Normally the Java side catches all exceptions and populates
// lastError. If that fails we check for an exception and try to
// populate last_error_ ourselves.
void TryHandleUncaughtException(JNIEnv* env, InnerPrivateData* private_data,
                                jthrowable exc) {
  jstring message =
      reinterpret_cast<jstring>(env->CallObjectMethod(exc, kObjectToStringMethod));
  if (!message) {
    private_data->last_error_.clear();
    return;
  }
  const char* str = env->GetStringUTFChars(message, 0);
  if (!str) {
    private_data->last_error_.clear();
    return;
  }
  private_data->last_error_ = str;
  env->ReleaseStringUTFChars(message, 0);
}

int ArrowArrayStreamGetSchema(ArrowArrayStream* stream, ArrowSchema* out) {
  assert(stream->private_data != nullptr);
  InnerPrivateData* private_data =
      reinterpret_cast<InnerPrivateData*>(stream->private_data);
  JNIEnvGuard guard(private_data->vm_);
  JNIEnv* env = guard.env();

  const jlong out_addr = static_cast<jlong>(reinterpret_cast<uintptr_t>(out));
  const int err_code = env->CallIntMethod(private_data->j_private_data_,
                                          kPrivateDataGetSchemaMethod, out_addr);
  if (jthrowable exc = env->ExceptionOccurred()) {
    TryHandleUncaughtException(env, private_data, exc);
    env->ExceptionClear();
    return EIO;
  } else if (err_code != 0) {
    TryCopyLastError(env, private_data);
  }
  return err_code;
}

int ArrowArrayStreamGetNext(ArrowArrayStream* stream, ArrowArray* out) {
  assert(stream->private_data != nullptr);
  InnerPrivateData* private_data =
      reinterpret_cast<InnerPrivateData*>(stream->private_data);
  JNIEnvGuard guard(private_data->vm_);
  JNIEnv* env = guard.env();

  const jlong out_addr = static_cast<jlong>(reinterpret_cast<uintptr_t>(out));
  const int err_code = env->CallIntMethod(private_data->j_private_data_,
                                          kPrivateDataGetNextMethod, out_addr);
  if (jthrowable exc = env->ExceptionOccurred()) {
    TryHandleUncaughtException(env, private_data, exc);
    env->ExceptionClear();
    return EIO;
  } else if (err_code != 0) {
    TryCopyLastError(env, private_data);
  }
  return err_code;
}

const char* ArrowArrayStreamGetLastError(ArrowArrayStream* stream) {
  assert(stream->private_data != nullptr);
  InnerPrivateData* private_data =
      reinterpret_cast<InnerPrivateData*>(stream->private_data);
  JNIEnvGuard guard(private_data->vm_);
  JNIEnv* env = guard.env();

  if (private_data->last_error_.empty()) return nullptr;
  return private_data->last_error_.c_str();
}

void ArrowArrayStreamRelease(ArrowArrayStream* stream) {
  // This should not be called on already released structure
  assert(stream->release != nullptr);
  // Release all data directly owned by the struct
  InnerPrivateData* private_data =
      reinterpret_cast<InnerPrivateData*>(stream->private_data);

  // It is possible for the JVM to be shut down (see above)
  try {
    JNIEnvGuard guard(private_data->vm_);
    JNIEnv* env = guard.env();

    env->CallObjectMethod(private_data->j_private_data_, kPrivateDataCloseMethod);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
    }
    env->DeleteGlobalRef(private_data->j_private_data_);
  } catch (const JniPendingException& e) {
    std::cerr << "WARNING: Failed to release Java ArrowArrayStream: " << e.what()
              << std::endl;
  }
  delete private_data;
  stream->private_data = nullptr;

  // Mark released
  stream->release = nullptr;
}

}  // namespace

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                \
  }                                                  \
  catch (JniPendingException & e) {                  \
    env->ThrowNew(kRuntimeExceptionClass, e.what()); \
    return fallback_expr;                            \
  }
// macro ended

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  JNI_METHOD_START
  kObjectClass = CreateGlobalClassReference(env, "Ljava/lang/Object;");
  kRuntimeExceptionClass =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");
  kPrivateDataClass =
      CreateGlobalClassReference(env, "Lorg/apache/arrow/c/jni/PrivateData;");
  kCDataExceptionClass =
      CreateGlobalClassReference(env, "Lorg/apache/arrow/c/jni/CDataJniException;");
  kStreamPrivateDataClass = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/c/ArrayStreamExporter$ExportedArrayStreamPrivateData;");

  kPrivateDataLastErrorField =
      GetFieldID(env, kStreamPrivateDataClass, "lastError", "[B");

  kObjectToStringMethod =
      GetMethodID(env, kObjectClass, "toString", "()Ljava/lang/String;");
  kPrivateDataCloseMethod = GetMethodID(env, kPrivateDataClass, "close", "()V");
  kPrivateDataGetNextMethod =
      GetMethodID(env, kStreamPrivateDataClass, "getNext", "(J)I");
  kPrivateDataGetSchemaMethod =
      GetMethodID(env, kStreamPrivateDataClass, "getSchema", "(J)I");
  kCDataExceptionConstructor =
      GetMethodID(env, kCDataExceptionClass, "<init>", "(ILjava/lang/String;)V");

  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(kObjectClass);
  env->DeleteGlobalRef(kRuntimeExceptionClass);
  env->DeleteGlobalRef(kPrivateDataClass);
  env->DeleteGlobalRef(kCDataExceptionClass);
  env->DeleteGlobalRef(kStreamPrivateDataClass);
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    releaseSchema
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_releaseSchema(
    JNIEnv* env, jobject, jlong address) {
  JNI_METHOD_START
  ArrowSchema* schema = reinterpret_cast<ArrowSchema*>(address);
  if (schema->release != nullptr) {
    schema->release(schema);
  }
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    releaseArray
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_c_jni_JniWrapper_releaseArray(JNIEnv* env, jobject, jlong address) {
  JNI_METHOD_START
  ArrowArray* array = reinterpret_cast<ArrowArray*>(address);
  if (array->release != nullptr) {
    array->release(array);
  }
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    getNextArrayStream
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_getNextArrayStream(
    JNIEnv* env, jobject, jlong address, jlong out_address) {
  JNI_METHOD_START
  auto* stream = reinterpret_cast<ArrowArrayStream*>(address);
  auto* out = reinterpret_cast<ArrowArray*>(out_address);
  const int err_code = stream->get_next(stream, out);
  if (err_code != 0) {
    const char* message = stream->get_last_error(stream);
    if (!message) message = std::strerror(err_code);
    jstring java_message = env->NewStringUTF(message);
    jthrowable exception = static_cast<jthrowable>(env->NewObject(
        kCDataExceptionClass, kCDataExceptionConstructor, err_code, java_message));
    env->Throw(exception);
  }
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    getSchemaArrayStream
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_getSchemaArrayStream(
    JNIEnv* env, jobject, jlong address, jlong out_address) {
  JNI_METHOD_START
  auto* stream = reinterpret_cast<ArrowArrayStream*>(address);
  auto* out = reinterpret_cast<ArrowSchema*>(out_address);
  const int err_code = stream->get_schema(stream, out);
  if (err_code != 0) {
    const char* message = stream->get_last_error(stream);
    if (!message) message = std::strerror(err_code);
    jstring java_message = env->NewStringUTF(message);
    jthrowable exception = static_cast<jthrowable>(env->NewObject(
        kCDataExceptionClass, kCDataExceptionConstructor, err_code, java_message));
    env->Throw(exception);
  }
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    releaseArrayStream
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_releaseArrayStream(
    JNIEnv* env, jobject, jlong address) {
  JNI_METHOD_START
  auto* stream = reinterpret_cast<ArrowArrayStream*>(address);
  if (stream->release != nullptr) {
    stream->release(stream);
  }
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    exportSchema
 * Signature: (JLorg/apache/arrow/c/jni/PrivateData;)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_exportSchema(
    JNIEnv* env, jobject, jlong address, jobject private_data) {
  JNI_METHOD_START
  ArrowSchema* schema = reinterpret_cast<ArrowSchema*>(address);

  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  jobject private_data_ref = env->NewGlobalRef(private_data);

  schema->private_data = new InnerPrivateData(vm, private_data_ref);
  schema->release = &release_exported<ArrowSchema>;
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    exportArray
 * Signature: (JLorg/apache/arrow/c/jni/PrivateData;)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_exportArray(
    JNIEnv* env, jobject, jlong address, jobject private_data) {
  JNI_METHOD_START
  ArrowArray* array = reinterpret_cast<ArrowArray*>(address);

  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  jobject private_data_ref = env->NewGlobalRef(private_data);

  array->private_data = new InnerPrivateData(vm, private_data_ref);
  array->release = &release_exported<ArrowArray>;
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_c_jni_JniWrapper
 * Method:    exportArrayStream
 * Signature: (JLorg/apache/arrow/c/jni/PrivateData;)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_c_jni_JniWrapper_exportArrayStream(
    JNIEnv* env, jobject, jlong address, jobject private_data) {
  JNI_METHOD_START
  auto* stream = reinterpret_cast<ArrowArrayStream*>(address);

  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  jobject private_data_ref = env->NewGlobalRef(private_data);

  stream->get_schema = &ArrowArrayStreamGetSchema;
  stream->get_next = &ArrowArrayStreamGetNext;
  stream->get_last_error = &ArrowArrayStreamGetLastError;
  stream->release = &ArrowArrayStreamRelease;
  stream->private_data = new InnerPrivateData(vm, private_data_ref);
  JNI_METHOD_END()
}
