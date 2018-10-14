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

#ifndef GANDIVA_FIELDDESCRIPTOR_H
#define GANDIVA_FIELDDESCRIPTOR_H

#include <string>

#include "gandiva/arrow.h"

namespace gandiva {

/// \brief Descriptor for an arrow field. Holds indexes into the flattened array of
/// buffers that is passed to LLVM generated functions.
class FieldDescriptor {
 public:
  static const int kInvalidIdx = -1;

  FieldDescriptor(FieldPtr field, int data_idx, int validity_idx = kInvalidIdx,
                  int offsets_idx = kInvalidIdx)
      : field_(field),
        data_idx_(data_idx),
        validity_idx_(validity_idx),
        offsets_idx_(offsets_idx) {}

  /// Index of validity array in the array-of-buffers
  int validity_idx() const { return validity_idx_; }

  /// Index of data array in the array-of-buffers
  int data_idx() const { return data_idx_; }

  /// Index of offsets array in the array-of-buffers
  int offsets_idx() const { return offsets_idx_; }

  FieldPtr field() const { return field_; }

  const std::string& Name() const { return field_->name(); }
  DataTypePtr Type() const { return field_->type(); }

  bool HasOffsetsIdx() const { return offsets_idx_ != kInvalidIdx; }

 private:
  FieldPtr field_;
  int data_idx_;
  int validity_idx_;
  int offsets_idx_;
};

}  // namespace gandiva

#endif  // GANDIVA_FIELDDESCRIPTOR_H
