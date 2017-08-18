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

#ifndef ARROW_PYTHON_PYTHON_TO_ARROW_INTERNAL_H
#define ARROW_PYTHON_PYTHON_TO_ARROW_INTERNAL_H

#include <vector>

#include "arrow/api.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {

#define UPDATE(OFFSET, TAG)               \
  if (TAG == -1) {                        \
    TAG = num_tags;                       \
    num_tags += 1;                        \
  }                                       \
  RETURN_NOT_OK(offsets_.Append(OFFSET)); \
  RETURN_NOT_OK(types_.Append(TAG));      \
  RETURN_NOT_OK(nones_.AppendToBitmap(true));

#define ADD_ELEMENT(VARNAME, TAG)                             \
  if (TAG != -1) {                                            \
    types[TAG] = std::make_shared<Field>("", VARNAME.type()); \
    RETURN_NOT_OK(VARNAME.Finish(&children[TAG]));            \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));               \
    type_ids.push_back(TAG);                                  \
  }

#define ADD_SUBSEQUENCE(DATA, OFFSETS, BUILDER, TAG, NAME)                          \
  if (DATA) {                                                                       \
    DCHECK(DATA->length() == OFFSETS.back());                                       \
    std::shared_ptr<Array> offset_array;                                            \
    Int32Builder builder(pool_, std::make_shared<Int32Type>());                     \
    RETURN_NOT_OK(builder.Append(OFFSETS.data(), OFFSETS.size()));                  \
    RETURN_NOT_OK(builder.Finish(&offset_array));                                   \
    std::shared_ptr<Array> list_array;                                              \
    ListArray::FromArrays(*offset_array, *DATA, pool_, &list_array);                \
    auto field = std::make_shared<Field>(NAME, list_array->type());                 \
    auto type =                                                                     \
        std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>({field})); \
    types[TAG] = std::make_shared<Field>("", type);                                 \
    children[TAG] = std::shared_ptr<StructArray>(                                   \
        new StructArray(type, list_array->length(), {list_array}));                 \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));                                     \
    type_ids.push_back(TAG);                                                        \
  } else {                                                                          \
    DCHECK_EQ(OFFSETS.size(), 1);                                                   \
  }

/// A Sequence is a heterogeneous collections of elements. It can contain
/// scalar Python types, lists, tuples, dictionaries and tensors.
class SequenceBuilder {
 public:
  explicit SequenceBuilder(MemoryPool* pool = nullptr)
      : pool_(pool),
        types_(pool, ::arrow::int8()),
        offsets_(pool, ::arrow::int32()),
        nones_(pool),
        bools_(pool, ::arrow::boolean()),
        ints_(pool, ::arrow::int64()),
        bytes_(pool, ::arrow::binary()),
        strings_(pool),
        floats_(pool, ::arrow::float32()),
        doubles_(pool, ::arrow::float64()),
        tensor_indices_(pool, ::arrow::int32()),
        list_offsets_({0}),
        tuple_offsets_({0}),
        dict_offsets_({0}) {}

  /// Appending a none to the sequence
  Status AppendNone() {
    RETURN_NOT_OK(offsets_.Append(0));
    RETURN_NOT_OK(types_.Append(0));
    return nones_.AppendToBitmap(false);
  }

  /// Appending a boolean to the sequence
  Status AppendBool(bool data) {
    UPDATE(bools_.length(), bool_tag);
    return bools_.Append(data);
  }

  /// Appending an int64_t to the sequence
  Status AppendInt64(int64_t data) {
    UPDATE(ints_.length(), int_tag);
    return ints_.Append(data);
  }

  /// Appending an uint64_t to the sequence
  Status AppendUInt64(uint64_t data) {
    UPDATE(ints_.length(), int_tag);
    return ints_.Append(data);
  }

  /// Append a list of bytes to the sequence
  Status AppendBytes(const uint8_t* data, int32_t length) {
    UPDATE(bytes_.length(), bytes_tag);
    return bytes_.Append(data, length);
  }

  /// Appending a string to the sequence
  Status AppendString(const char* data, int32_t length) {
    UPDATE(strings_.length(), string_tag);
    return strings_.Append(data, length);
  }

  /// Appending a float to the sequence
  Status AppendFloat(float data) {
    UPDATE(floats_.length(), float_tag);
    return floats_.Append(data);
  }

  /// Appending a double to the sequence
  Status AppendDouble(double data) {
    UPDATE(doubles_.length(), double_tag);
    return doubles_.Append(data);
  }

  /// Appending a tensor to the sequence
  ///
  /// \param tensor_index Index of the tensor in the object.
  Status AppendTensor(int32_t tensor_index) {
    UPDATE(tensor_indices_.length(), tensor_tag);
    return tensor_indices_.Append(tensor_index);
  }

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
  Status AppendList(int32_t size) {
    UPDATE(list_offsets_.size() - 1, list_tag);
    list_offsets_.push_back(list_offsets_.back() + size);
    return Status::OK();
  }

  Status AppendTuple(int32_t size) {
    UPDATE(tuple_offsets_.size() - 1, tuple_tag);
    tuple_offsets_.push_back(tuple_offsets_.back() + size);
    return Status::OK();
  }

  Status AppendDict(int32_t size) {
    UPDATE(dict_offsets_.size() - 1, dict_tag);
    dict_offsets_.push_back(dict_offsets_.back() + size);
    return Status::OK();
  }

  /// Finish building the sequence and return the result.
  Status Finish(std::shared_ptr<Array> list_data, std::shared_ptr<Array> tuple_data,
                std::shared_ptr<Array> dict_data, std::shared_ptr<Array>* out) {
    std::vector<std::shared_ptr<Field>> types(num_tags);
    std::vector<std::shared_ptr<Array>> children(num_tags);
    std::vector<uint8_t> type_ids;

    ADD_ELEMENT(bools_, bool_tag);
    ADD_ELEMENT(ints_, int_tag);
    ADD_ELEMENT(strings_, string_tag);
    ADD_ELEMENT(bytes_, bytes_tag);
    ADD_ELEMENT(floats_, float_tag);
    ADD_ELEMENT(doubles_, double_tag);

    ADD_ELEMENT(tensor_indices_, tensor_tag);

    ADD_SUBSEQUENCE(list_data, list_offsets_, list_builder, list_tag, "list");
    ADD_SUBSEQUENCE(tuple_data, tuple_offsets_, tuple_builder, tuple_tag, "tuple");
    ADD_SUBSEQUENCE(dict_data, dict_offsets_, dict_builder, dict_tag, "dict");

    auto type = ::arrow::union_(types, type_ids, UnionMode::DENSE);
    out->reset(new UnionArray(type, types_.length(), children, types_.data(),
                              offsets_.data(), nones_.null_bitmap(), nones_.null_count()));
    return Status::OK();
  }

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

/// Constructing dictionaries of key/value pairs. Sequences of
/// keys and values are built separately using a pair of
/// SequenceBuilders. The resulting Arrow representation
/// can be obtained via the Finish method.
class DictBuilder {
 public:
  explicit DictBuilder(MemoryPool* pool = nullptr) : keys_(pool), vals_(pool) {}

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
  Status Finish(std::shared_ptr<Array> key_tuple_data,
                std::shared_ptr<Array> key_dict_data,
                std::shared_ptr<Array> val_list_data,
                std::shared_ptr<Array> val_tuple_data,
                std::shared_ptr<Array> val_dict_data,
                std::shared_ptr<Array>* out) {
    // lists and dicts can't be keys of dicts in Python, that is why for
    // the keys we do not need to collect sublists
    std::shared_ptr<Array> keys, vals;
    RETURN_NOT_OK(keys_.Finish(nullptr, key_tuple_data, key_dict_data, &keys));
    RETURN_NOT_OK(vals_.Finish(val_list_data, val_tuple_data, val_dict_data, &vals));
    auto keys_field = std::make_shared<Field>("keys", keys->type());
    auto vals_field = std::make_shared<Field>("vals", vals->type());
    auto type = std::make_shared<StructType>(
        std::vector<std::shared_ptr<Field>>({keys_field, vals_field}));
    std::vector<std::shared_ptr<Array>> field_arrays({keys, vals});
    DCHECK(keys->length() == vals->length());
    out->reset(new StructArray(type, keys->length(), field_arrays));
    return Status::OK();
  }

 private:
  SequenceBuilder keys_;
  SequenceBuilder vals_;
};

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYTHON_TO_ARROW_INTERNAL_H
