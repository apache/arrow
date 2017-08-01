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

#ifndef PYTHON_ARROW_DICT_H
#define PYTHON_ARROW_DICT_H

#include <arrow/api.h>

#include "sequence.h"

namespace arrow {

/// Constructing dictionaries of key/value pairs. Sequences of
/// keys and values are built separately using a pair of
/// SequenceBuilders. The resulting Arrow representation
/// can be obtained via the Finish method.
class DictBuilder {
 public:
  DictBuilder(arrow::MemoryPool* pool = nullptr) : keys_(pool), vals_(pool) {}

  /// Builder for the keys of the dictionary
  SequenceBuilder& keys() { return keys_; }
  /// Builder for the values of the dictionary
  SequenceBuilder& vals() { return vals_; }

  /// Construct an Arrow StructArray representing the dictionary.
  /// Contains a field "keys" for the keys and "vals" for the values.

  /// \param list_data
  ///    List containing the data from nested lists in the value
  ///   list of the dictionary
  ///
  /// \param dict_data
  ///   List containing the data from nested dictionaries in the
  ///   value list of the dictionary
  arrow::Status Finish(std::shared_ptr<arrow::Array> key_tuple_data,
      std::shared_ptr<arrow::Array> key_dict_data,
      std::shared_ptr<arrow::Array> val_list_data,
      std::shared_ptr<arrow::Array> val_tuple_data,
      std::shared_ptr<arrow::Array> val_dict_data, std::shared_ptr<arrow::Array>* out);

 private:
  SequenceBuilder keys_;
  SequenceBuilder vals_;
};

} // namespace arrow

#endif  // PYARROW_DICT_H
