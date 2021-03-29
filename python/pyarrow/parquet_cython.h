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

#include <map>
#include <memory>
#include <string>

#include "parquet/encryption.h"

// Utility classes to help C++ code talk to Cython, since Cython doesn't allow
// subclassing C++ classes (unlike, say, pybind11).
namespace parquet {

// This will be a Cython function that calls a Python function. We return actual
// string since otherwise ownership is a concern when the original Python
// bytestring gets destroyed.
using CythonKeyRetrieverFunc = std::string (*)(void*, const std::string&);

// DecryptionKeyRetriever that calls back into Python.
class CythonDecryptionKeyRetriever : public DecryptionKeyRetriever {
 public:
  CythonDecryptionKeyRetriever(void* object, CythonKeyRetrieverFunc callable)
      : py_object_(object), callable_(callable) {}

  std::string GetKey(const std::string& key_metadata) const {
    std::string key = callable_(py_object_, key_metadata);
    // We store the key to preserve ownership when original Python object goes
    // away.
    key_map_.insert({key_metadata, key});
    return key_map_.at(key_metadata);
  }

  static std::shared_ptr<CythonDecryptionKeyRetriever> build(
      void* object, CythonKeyRetrieverFunc callable) {
    return std::shared_ptr<CythonDecryptionKeyRetriever>(
        new CythonDecryptionKeyRetriever(object, callable));
  }

 private:
  void* py_object_;
  CythonKeyRetrieverFunc callable_;
  mutable std::map<std::string, std::string> key_map_;
};

}  // namespace parquet
