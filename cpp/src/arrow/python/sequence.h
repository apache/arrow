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

#ifndef PYTHON_ARROW_SEQUENCE_H
#define PYTHON_ARROW_SEQUENCE_H

#include <vector>

#include "arrow/api.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {

/// A Sequence is a heterogeneous collections of elements. It can contain
/// scalar Python types, lists, tuples, dictionaries and tensors.
class SequenceBuilder {
 public:
  explicit SequenceBuilder(MemoryPool* pool = nullptr);

  /// Appending a none to the sequence
  Status AppendNone();

  /// Appending a boolean to the sequence
  Status AppendBool(bool data);

  /// Appending an int64_t to the sequence
  Status AppendInt64(int64_t data);

  /// Appending an uint64_t to the sequence
  Status AppendUInt64(uint64_t data);

  /// Append a list of bytes to the sequence
  Status AppendBytes(const uint8_t* data, int32_t length);

  /// Appending a string to the sequence
  Status AppendString(const char* data, int32_t length);

  /// Appending a float to the sequence
  Status AppendFloat(float data);

  /// Appending a double to the sequence
  Status AppendDouble(double data);

  /// Appending a tensor to the sequence
  ///
  /// \param tensor_index Index of the tensor in the object.
  Status AppendTensor(int32_t tensor_index);

  /// Add a sublist to the sequence. The data contained in the sublist will be
  /// specified in the "Finish" method.
  ///
  /// To construct l = [[11, 22], 33, [44, 55]] you would for example run
  /// list = ListBuilder();
  /// list.AppendList(2);
  /// list.Append(33);
  /// list.AppendList(2);
  /// list.Finish([11, 22, 44, 55]);
  /// list.Finish();

  /// \param size
  /// The size of the sublist
  Status AppendList(int32_t size);

  Status AppendTuple(int32_t size);

  Status AppendDict(int32_t size);

  /// Finish building the sequence and return the result.
  Status Finish(std::shared_ptr<Array> list_data, std::shared_ptr<Array> tuple_data,
                std::shared_ptr<Array> dict_data, std::shared_ptr<Array>* out);

 private:
  MemoryPool* pool_;

  Int8Builder types_;
  Int32Builder offsets_;

  /// Total number of bytes needed to represent this sequence.
  int64_t total_num_bytes_;

  NullBuilder nones_;
  BooleanBuilder bools_;
  Int64Builder ints_;
  BinaryBuilder bytes_;
  StringBuilder strings_;
  FloatBuilder floats_;
  DoubleBuilder doubles_;

  // We use an Int32Builder here to distinguish the tensor indices from
  // the ints_ above (see the case Type::INT32 in get_value in python.cc).
  // TODO(pcm): Replace this by using the union tags to distinguish between
  // these two cases.
  Int32Builder tensor_indices_;

  std::vector<int32_t> list_offsets_;
  std::vector<int32_t> tuple_offsets_;
  std::vector<int32_t> dict_offsets_;

  // Tags for members of the sequence. If they are set to -1 it means
  // they are not used and will not part be of the metadata when we call
  // SequenceBuilder::Finish. If a member with one of the tags is added,
  // the associated variable gets a unique index starting from 0. This
  // happens in the UPDATE macro in sequence.cc.
  int8_t bool_tag = -1;
  int8_t int_tag = -1;
  int8_t string_tag = -1;
  int8_t bytes_tag = -1;
  int8_t float_tag = -1;
  int8_t double_tag = -1;

  int8_t tensor_tag = -1;
  int8_t list_tag = -1;
  int8_t tuple_tag = -1;
  int8_t dict_tag = -1;

  int8_t num_tags = 0;
};

}  // namespace py
}  // namespace arrow

#endif  // PYTHON_ARROW_SEQUENCE_H
