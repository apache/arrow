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
#include <memory>
#include <stdexcept>
#include <string>

#include "./abi.h"
#include "org_apache_arrow_c_jni_JniWrapper.h"

namespace {

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jclass illegal_access_exception_class;
jclass illegal_argument_exception_class;
jclass runtime_exception_class;
jclass private_data_class;

jmethodID private_data_close_method;

jint JNI_VERSION = JNI_VERSION_1_6;

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : std::runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

void JniThrow(std::string message) { ThrowPendingException(message); }

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature " + std::string(sig);
    ThrowPendingException(error_message);
  }
  return ret;
}

class InnerPrivateData {
 public:
  InnerPrivateData(JavaVM* vm, jobject private_data)
      : vm_(vm), j_private_data_(private_data) {}

  JavaVM* vm_;
  jobject j_private_data_;
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

  JNIEnvGuard guard(private_data->vm_);
  JNIEnv* env = guard.env();

  env->CallObjectMethod(private_data->j_private_data_, private_data_close_method);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    ThrowPendingException("Error calling close of private data");
  }
  env->DeleteGlobalRef(private_data->j_private_data_);
  delete private_data;
  base->private_data = nullptr;

  // Mark released
  base->release = nullptr;
}
}  // namespace

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
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
  private_data_class =
      CreateGlobalClassReference(env, "Lorg/apache/arrow/c/jni/PrivateData;");

  private_data_close_method = GetMethodID(env, private_data_class, "close", "()V");

  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
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
