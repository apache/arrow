// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <string>

#include "jni/org_apache_arrow_gandiva_evaluator_ConfigurationBuilder.h"
#include "gandiva/configuration.h"
#include "jni/config_holder.h"

using gandiva::Configuration;
using gandiva::ConfigHolder;
using gandiva::ConfigurationBuilder;

jclass configuration_builder_class_;
jmethodID byte_code_accessor_method_id_;

/*
 * Class:     org_apache_arrow_gandiva_evaluator_ConfigBuilder
 * Method:    buildConfigInstance
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_gandiva_evaluator_ConfigurationBuilder_buildConfigInstance
(JNIEnv * env, jobject configuration) {
  if (configuration_builder_class_ == nullptr) {
    const char class_name[] = "org/apache/arrow/gandiva/evaluator/ConfigurationBuilder";
    configuration_builder_class_ = env->FindClass(class_name);
  }

  if (byte_code_accessor_method_id_ == nullptr) {
    const char method_name[] = "getByteCodeFilePath";
    const char return_type[] = "()Ljava/lang/String;";
    byte_code_accessor_method_id_ = env->GetMethodID(configuration_builder_class_,
                                                     method_name, return_type);
  }

  jstring byte_code_file_path = (jstring) env->CallObjectMethod(configuration,
                                                 byte_code_accessor_method_id_, 0);
  ConfigurationBuilder configuration_builder;
  if (byte_code_file_path != nullptr) {
    const char *byte_code_file_path_cpp = env->GetStringUTFChars(byte_code_file_path, 0);
    configuration_builder.set_byte_code_file_path(byte_code_file_path_cpp);
  }
  std::shared_ptr<Configuration> config = configuration_builder.build();
  return ConfigHolder::MapInsert(config);
}

/*
 * Class:     org_apache_arrow_gandiva_evaluator_ConfigBuilder
 * Method:    releaseConfigInstance
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_gandiva_evaluator_ConfigurationBuilder_releaseConfigInstance
(JNIEnv *env, jobject configuration, jlong config_id) {
  ConfigHolder::MapErase(config_id);
}
