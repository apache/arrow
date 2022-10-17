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

#include "gandiva/annotator.h"

#include <memory>
#include <string>

#include "gandiva/field_descriptor.h"

namespace gandiva {

FieldDescriptorPtr Annotator::CheckAndAddInputFieldDescriptor(FieldPtr field) {
  // If the field is already in the map, return the entry.
  auto found = in_name_to_desc_.find(field->name());
  if (found != in_name_to_desc_.end()) {
    return found->second;
  }

  auto desc = MakeDesc(field, false /*is_output*/);
  in_name_to_desc_[field->name()] = desc;
  return desc;
}

FieldDescriptorPtr Annotator::AddOutputFieldDescriptor(FieldPtr field) {
  auto desc = MakeDesc(field, true /*is_output*/);
  out_descs_.push_back(desc);
  return desc;
}

FieldDescriptorPtr Annotator::MakeDesc(FieldPtr field, bool is_output) {
  int data_idx = buffer_count_++;
  int validity_idx = buffer_count_++;
  int offsets_idx = FieldDescriptor::kInvalidIdx;
  if (arrow::is_binary_like(field->type()->id())) {
    offsets_idx = buffer_count_++;
  }
  int data_buffer_ptr_idx = FieldDescriptor::kInvalidIdx;
  if (is_output) {
    data_buffer_ptr_idx = buffer_count_++;
  }
  return std::make_shared<FieldDescriptor>(field, data_idx, validity_idx, offsets_idx,
                                           data_buffer_ptr_idx);
}

int Annotator::AddHolderPointer(void* holder) {
  int size = static_cast<int>(holder_pointers_.size());
  holder_pointers_.push_back(holder);
  return size;
}

void Annotator::PrepareBuffersForField(const FieldDescriptor& desc,
                                       const arrow::ArrayData& array_data,
                                       EvalBatch* eval_batch, bool is_output) const {
  int buffer_idx = 0;

  // The validity buffer is optional. Use nullptr if it does not have one.
  if (array_data.buffers[buffer_idx]) {
    uint8_t* validity_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
    eval_batch->SetBuffer(desc.validity_idx(), validity_buf, array_data.offset);
  } else {
    eval_batch->SetBuffer(desc.validity_idx(), nullptr, array_data.offset);
  }
  ++buffer_idx;

  if (desc.HasOffsetsIdx()) {
    uint8_t* offsets_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
    eval_batch->SetBuffer(desc.offsets_idx(), offsets_buf, array_data.offset);
    ++buffer_idx;
  }

  uint8_t* data_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
  eval_batch->SetBuffer(desc.data_idx(), data_buf, array_data.offset);
  if (is_output) {
    // pass in the Buffer object for output data buffers. Can be used for resizing.
    uint8_t* data_buf_ptr =
        reinterpret_cast<uint8_t*>(array_data.buffers[buffer_idx].get());
    eval_batch->SetBuffer(desc.data_buffer_ptr_idx(), data_buf_ptr, array_data.offset);
  }
}

EvalBatchPtr Annotator::PrepareEvalBatch(const arrow::RecordBatch& record_batch,
                                         const ArrayDataVector& out_vector) const {
  EvalBatchPtr eval_batch = std::make_shared<EvalBatch>(
      record_batch.num_rows(), buffer_count_, local_bitmap_count_);

  // Fill in the entries for the input fields.
  for (int i = 0; i < record_batch.num_columns(); ++i) {
    const std::string& name = record_batch.column_name(i);
    auto found = in_name_to_desc_.find(name);
    if (found == in_name_to_desc_.end()) {
      // skip columns not involved in the expression.
      continue;
    }

    PrepareBuffersForField(*(found->second), *(record_batch.column_data(i)),
                           eval_batch.get(), false /*is_output*/);
  }

  // Fill in the entries for the output fields.
  int idx = 0;
  for (auto& arraydata : out_vector) {
    const FieldDescriptorPtr& desc = out_descs_.at(idx);
    PrepareBuffersForField(*desc, *arraydata, eval_batch.get(), true /*is_output*/);
    ++idx;
  }
  return eval_batch;
}

}  // namespace gandiva
