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

#include "arrow/table/column.h"

#include <memory>
#include <sstream>

#include "arrow/type.h"
#include "arrow/util/status.h"

namespace arrow {

ChunkedArray::ChunkedArray(const ArrayVector& chunks) :
    chunks_(chunks) {
  length_ = 0;
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

Column::Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks) :
    field_(field) {
  data_ = std::make_shared<ChunkedArray>(chunks);
}

Column::Column(const std::shared_ptr<Field>& field,
    const std::shared_ptr<Array>& data) :
    field_(field) {
  data_ = std::make_shared<ChunkedArray>(ArrayVector({data}));
}

Column::Column(const std::shared_ptr<Field>& field,
    const std::shared_ptr<ChunkedArray>& data) :
    field_(field),
    data_(data) {}

Status Column::ValidateData() {
  for (int i = 0; i < data_->num_chunks(); ++i) {
    const std::shared_ptr<DataType>& type = data_->chunk(i)->type();
    if (!this->type()->Equals(type)) {
      std::stringstream ss;
      ss << "In chunk " << i << " expected type "
         << this->type()->ToString()
         << " but saw "
         << type->ToString();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

} // namespace arrow
