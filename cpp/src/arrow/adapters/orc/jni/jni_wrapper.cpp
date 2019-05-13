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

#include <string>
#include <arrow/io/api.h>

#include "org_apache_arrow_adapter_orc_OrcReaderJniWrapper.h"
#include "org_apache_arrow_adapter_orc_OrcReaderJniWrapper_StripeReader.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_adapter_orc_OrcReaderJniWrapper_open
        (JNIEnv *env, jobject this_obj, jstring file_path) {
    auto in_file = new std::shared_ptr<arrow::io::ReadableFile>();
    const char *str= env->GetStringUTFChars(file_path, nullptr);
    std::string path(str);
    env->ReleaseStringUTFChars(file_path, str);

    arrow::Status ret;
    if (path.find("hdfs://") == 0) {
        return false;
    } else {
        ret = arrow::io::ReadableFile::Open(path, in_file);
    }

    if(ret.ok()) {

        jclass this_class = env->GetObjectClass(this_obj);
        jfieldID fidNativeManager = env->GetFieldID(this_class, "nativeReaderAddress", "J");
        if (fidNativeManager == nullptr)
        {
            return false;
        }

        env->SetLongField(this_obj, fidNativeManager, (long)in_file);
    }

    return ret.ok();
}

#ifdef __cplusplus
}
#endif
