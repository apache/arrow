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

// Interfaces to use for defining Flight RPC servers. API should be considered
// experimental for now

#pragma once

#include <boost/variant.hpp>

using string_list_t = std::vector<std::string>;
using int32_to_int32_list_t = std::unordered_map<int32_t, std::vector<int32_t>>;
using SqlInfoResult = boost::variant<std::string, bool, int64_t, int32_t, string_list_t,
                                     int32_to_int32_list_t>;
using sql_info_id_to_result_t = std::unordered_map<int32_t, SqlInfoResult>;

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

/// \brief Auxiliary class used to populate GetSqlInfo's DenseUnionArray with different
/// data types.
class SqlInfoResultAppender : public boost::static_visitor<Status> {
 public:
  /// \brief Appends a string to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(const std::string& value);

  /// \brief Appends a bool to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(bool value);

  /// \brief Appends a int64_t to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(int64_t value);

  /// \brief Appends a int64_t to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(int32_t value);

  /// \brief Appends a string list to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(const string_list_t& value);

  /// \brief Appends a int32 to int32 list map to the DenseUnionBuilder.
  /// \param[in] value Value to be appended.
  Status operator()(const int32_to_int32_list_t& value);

  /// \brief Creates a boost::variant visitor that appends data to given
  /// DenseUnionBuilder. \param[in] value_builder  DenseUnionBuilder to append data to.
  explicit SqlInfoResultAppender(DenseUnionBuilder& value_builder);

 private:
  DenseUnionBuilder& value_builder_;

  enum : int8_t {
    STRING_VALUE_INDEX = 0,
    BOOL_VALUE_INDEX = 1,
    BIGINT_VALUE_INDEX = 2,
    INT32_BITMASK_INDEX = 3,
    STRING_LIST_INDEX = 4,
    INT32_TO_INT32_LIST_INDEX = 5
  };
};

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
