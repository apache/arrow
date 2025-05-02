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

// Implement a simple JSON representation format for arrays

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;

namespace json {

/// \defgroup array-from-json Helpers for constructing Arrays from JSON text
///
/// These helpers are intended to be used in examples, tests, or for quick
/// prototyping and are not intended to be used where performance matters.
///
/// @{

/// \brief Create an Array from a JSON string
ARROW_EXPORT
Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>&,
                                                   const std::string& json);

/// \brief Create an Array from a JSON string
ARROW_EXPORT
Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>&,
                                                   std::string_view json);

/// \brief Create an Array from a JSON string
ARROW_EXPORT
Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>&,
                                                   const char* json);

/// \brief Create an ChunkedArray from a JSON string
ARROW_EXPORT
Status ChunkedArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                  const std::vector<std::string>& json_strings,
                                  std::shared_ptr<ChunkedArray>* out);

/// \brief Create an DictionaryArray from a JSON string
ARROW_EXPORT
Status DictArrayFromJSONString(const std::shared_ptr<DataType>&,
                               std::string_view indices_json,
                               std::string_view dictionary_json,
                               std::shared_ptr<Array>* out);

/// \brief Create an Scalar from a JSON string
ARROW_EXPORT
Status ScalarFromJSONString(const std::shared_ptr<DataType>&, std::string_view json,
                            std::shared_ptr<Scalar>* out);

/// \brief Create an DictionaryScalar from a JSON string
ARROW_EXPORT
Status DictScalarFromJSONString(const std::shared_ptr<DataType>&,
                                std::string_view index_json,
                                std::string_view dictionary_json,
                                std::shared_ptr<Scalar>* out);

/// @}

}  // namespace json
}  // namespace arrow
