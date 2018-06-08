// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <string>
#include "codegen/annotator.h"
#include "codegen/field_descriptor.h"

namespace gandiva {

FieldDescriptorPtr Annotator::CheckAndAddInputFieldDescriptor(FieldPtr field) {
  // If the field is already in the map, return the entry.
  auto found = in_name_to_desc_.find(field->name());
  if (found != in_name_to_desc_.end()) {
    return found->second;
  }

  auto desc = MakeDesc(field);
  in_name_to_desc_[field->name()] = desc;
  return desc;
}

FieldDescriptorPtr Annotator::AddOutputFieldDescriptor(FieldPtr field) {
  auto desc = MakeDesc(field);
  out_descs_.push_back(desc);
  return desc;
}

FieldDescriptorPtr Annotator::MakeDesc(FieldPtr field) {
  // TODO:
  // - validity is optional
  // - may have offsets also
  int data_idx = buffer_count_++;
  int validity_idx = buffer_count_++;
  return std::make_shared<FieldDescriptor>(field, data_idx, validity_idx);
}

void Annotator::PrepareBuffersForField(const FieldDescriptor &desc,
                                       const arrow::ArrayData &array_data,
                                       EvalBatch *eval_batch) {
  // TODO:
  // - validity is optional
  // - may have offsets also

  uint8_t *validity_buf = const_cast<uint8_t *>(array_data.buffers[0]->data());
  eval_batch->SetBuffer(desc.validity_idx(), validity_buf);

  uint8_t *data_buf = const_cast<uint8_t *>(array_data.buffers[1]->data());
  eval_batch->SetBuffer(desc.data_idx(), data_buf);
}

EvalBatchPtr Annotator::PrepareEvalBatch(const arrow::RecordBatch &record_batch,
                                         const ArrayDataVector &out_vector) {
  EvalBatchPtr eval_batch = std::make_shared<EvalBatch>(record_batch.num_rows(),
                                                        buffer_count_,
                                                        local_bitmap_count_);

  // Fill in the entries for the input fields.
  for (int i = 0; i < record_batch.num_columns(); ++i) {
    const std::string &name = record_batch.column_name(i);
    auto found = in_name_to_desc_.find(name);
    if (found == in_name_to_desc_.end()) {
      // skip columns not involved in the expression.
      continue;
    }

    PrepareBuffersForField(*(found->second),
                           *(record_batch.column(i))->data(),
                           eval_batch.get());
  }

  // Fill in the entries for the output fields.
  int idx = 0;
  for (auto &arraydata : out_vector) {
    const FieldDescriptorPtr &desc = out_descs_.at(idx);
    PrepareBuffersForField(*desc, *arraydata, eval_batch.get());
    ++idx;
  }
  return eval_batch;
}

} // namespace gandiva
