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

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"

namespace arrow {

/// Concrete Array class for union data
class ARROW_EXPORT UnionArray : public Array {
 public:
  using TypeClass = UnionType;

  using type_code_t = int8_t;

  explicit UnionArray(const std::shared_ptr<ArrayData>& data);

  UnionArray(const std::shared_ptr<DataType>& type, int64_t length,
             const std::vector<std::shared_ptr<Array>>& children,
             const std::shared_ptr<Buffer>& type_ids,
             const std::shared_ptr<Buffer>& value_offsets = NULLPTR,
             const std::shared_ptr<Buffer>& null_bitmap = NULLPTR,
             int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  /// \brief Construct Dense UnionArray from types_ids, value_offsets and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types. The value_offsets are assumed to be well-formed.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] value_offsets An array of signed int32 values indicating the
  /// relative offset into the respective child array for the type in a given slot.
  /// The respective offsets for each child value array must be in order / increasing.
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] field_names Vector of strings containing the name of each field.
  /// \param[in] type_codes Vector of type codes.
  /// \param[out] out Will have length equal to value_offsets.length()
  static Status MakeDense(const Array& type_ids, const Array& value_offsets,
                          const std::vector<std::shared_ptr<Array>>& children,
                          const std::vector<std::string>& field_names,
                          const std::vector<type_code_t>& type_codes,
                          std::shared_ptr<Array>* out);

  /// \brief Construct Dense UnionArray from types_ids, value_offsets and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types. The value_offsets are assumed to be well-formed.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] value_offsets An array of signed int32 values indicating the
  /// relative offset into the respective child array for the type in a given slot.
  /// The respective offsets for each child value array must be in order / increasing.
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] field_names Vector of strings containing the name of each field.
  /// \param[out] out Will have length equal to value_offsets.length()
  static Status MakeDense(const Array& type_ids, const Array& value_offsets,
                          const std::vector<std::shared_ptr<Array>>& children,
                          const std::vector<std::string>& field_names,
                          std::shared_ptr<Array>* out) {
    return MakeDense(type_ids, value_offsets, children, field_names, {}, out);
  }

  /// \brief Construct Dense UnionArray from types_ids, value_offsets and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types. The value_offsets are assumed to be well-formed.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] value_offsets An array of signed int32 values indicating the
  /// relative offset into the respective child array for the type in a given slot.
  /// The respective offsets for each child value array must be in order / increasing.
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] type_codes Vector of type codes.
  /// \param[out] out Will have length equal to value_offsets.length()
  static Status MakeDense(const Array& type_ids, const Array& value_offsets,
                          const std::vector<std::shared_ptr<Array>>& children,
                          const std::vector<type_code_t>& type_codes,
                          std::shared_ptr<Array>* out) {
    return MakeDense(type_ids, value_offsets, children, {}, type_codes, out);
  }

  /// \brief Construct Dense UnionArray from types_ids, value_offsets and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types. The value_offsets are assumed to be well-formed.
  ///
  /// The name of each field is filled by the index of the field.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] value_offsets An array of signed int32 values indicating the
  /// relative offset into the respective child array for the type in a given slot.
  /// The respective offsets for each child value array must be in order / increasing.
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[out] out Will have length equal to value_offsets.length()
  static Status MakeDense(const Array& type_ids, const Array& value_offsets,
                          const std::vector<std::shared_ptr<Array>>& children,
                          std::shared_ptr<Array>* out) {
    return MakeDense(type_ids, value_offsets, children, {}, {}, out);
  }

  /// \brief Construct Sparse UnionArray from type_ids and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] field_names Vector of strings containing the name of each field.
  /// \param[in] type_codes Vector of type codes.
  /// \param[out] out Will have length equal to type_ids.length()
  static Status MakeSparse(const Array& type_ids,
                           const std::vector<std::shared_ptr<Array>>& children,
                           const std::vector<std::string>& field_names,
                           const std::vector<type_code_t>& type_codes,
                           std::shared_ptr<Array>* out);

  /// \brief Construct Sparse UnionArray from type_ids and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] field_names Vector of strings containing the name of each field.
  /// \param[out] out Will have length equal to type_ids.length()
  static Status MakeSparse(const Array& type_ids,
                           const std::vector<std::shared_ptr<Array>>& children,
                           const std::vector<std::string>& field_names,
                           std::shared_ptr<Array>* out) {
    return MakeSparse(type_ids, children, field_names, {}, out);
  }

  /// \brief Construct Sparse UnionArray from type_ids and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[in] type_codes Vector of type codes.
  /// \param[out] out Will have length equal to type_ids.length()
  static Status MakeSparse(const Array& type_ids,
                           const std::vector<std::shared_ptr<Array>>& children,
                           const std::vector<type_code_t>& type_codes,
                           std::shared_ptr<Array>* out) {
    return MakeSparse(type_ids, children, {}, type_codes, out);
  }

  /// \brief Construct Sparse UnionArray from type_ids and children
  ///
  /// This function does the bare minimum of validation of the offsets and
  /// input types.
  ///
  /// The name of each field is filled by the index of the field.
  ///
  /// \param[in] type_ids An array of logical type ids for the union type
  /// \param[in] children Vector of children Arrays containing the data for each type.
  /// \param[out] out Will have length equal to type_ids.length()
  static Status MakeSparse(const Array& type_ids,
                           const std::vector<std::shared_ptr<Array>>& children,
                           std::shared_ptr<Array>* out) {
    return MakeSparse(type_ids, children, {}, {}, out);
  }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> type_codes() const { return data_->buffers[1]; }

  const type_code_t* raw_type_codes() const { return raw_type_codes_ + data_->offset; }

  ARROW_DEPRECATED("Use UnionArray::type_codes")
  std::shared_ptr<Buffer> type_ids() const { return type_codes(); }

  ARROW_DEPRECATED("Use UnionArray::raw_type_codes")
  const type_code_t* raw_type_ids() const { return raw_type_codes(); }

  /// The physical child id containing value at index.
  int child_id(int64_t i) const {
    return union_type_->child_ids()[raw_type_codes_[i + data_->offset]];
  }

  /// For dense arrays only.
  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[2]; }

  /// For dense arrays only.
  int32_t value_offset(int64_t i) const { return raw_value_offsets_[i + data_->offset]; }

  /// For dense arrays only.
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + data_->offset; }

  const UnionType* union_type() const { return union_type_; }

  UnionMode::type mode() const { return union_type_->mode(); }

  // Return the given field as an individual array.
  // For sparse unions, the returned array has its offset, length and null
  // count adjusted.
  // For dense unions, the returned array is unchanged.
  std::shared_ptr<Array> child(int pos) const;

 protected:
  void SetData(const std::shared_ptr<ArrayData>& data);

  const type_code_t* raw_type_codes_;
  const int32_t* raw_value_offsets_;
  const UnionType* union_type_;

  // For caching boxed child data
  mutable std::vector<std::shared_ptr<Array>> boxed_fields_;
};

}  // namespace arrow
