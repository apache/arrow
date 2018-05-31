/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_EXPR_EVALBATCH_H
#define GANDIVA_EXPR_EVALBATCH_H

#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief The buffers corresponding to one batch of records, used for
/// expression evaluation.
class EvalBatch {
 public:
  explicit EvalBatch(int num_buffers)
    : num_buffers_(num_buffers) {
    buffers_ = new uint8_t *[num_buffers];
  }

  ~EvalBatch() {
    delete [] buffers_;
  }

  uint8_t **buffers() { return buffers_; }

  int num_buffers() { return num_buffers_; }

  void SetBufferAtIdx(int idx, uint8_t *buffer) {
    DCHECK(idx <= num_buffers_);
    buffers_[idx] = buffer;
  }

 private:
  /// An array of 'num_buffers_', each containing a buffer. The buffer
  /// sizes depends on the data type, but all of them have the same
  /// number of slots (equal to num_rows in the RecordBatch).
  uint8_t **buffers_;
  int num_buffers_;
};

} // namespace gandiva

#endif //GANDIVA_EXPR_EVALBATCH_H
