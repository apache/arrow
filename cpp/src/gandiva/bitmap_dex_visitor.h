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
#ifndef GANDIVA_BITMAP_DEXVISITOR_H
#define GANDIVA_BITMAP_DEXVISITOR_H

#include "codegen/dex_visitor.h"
#include "codegen/dex.h"
#include "codegen/eval_batch.h"

namespace gandiva {

/// \brief Extract bitmap buffer from either the input/buffer vectors or the
/// local validity bitmap.
class BitMapDexVisitor : public DexDefaultVisitor {
 public:
  explicit BitMapDexVisitor(const EvalBatch &eval_batch)
    : eval_batch_(eval_batch),
      bitmap_(nullptr) {}

  void Visit(const VectorReadValidityDex &dex) {
    int idx = dex.ValidityIdx();
    bitmap_ = eval_batch_.GetBuffer(idx);
  }

  void Visit(const LocalBitMapValidityDex &dex) {
    int idx = dex.local_bitmap_idx();
    bitmap_ = eval_batch_.GetLocalBitMap(idx);
  }

  uint8_t *bitmap() { return bitmap_; }

 private:
  const EvalBatch &eval_batch_;
  uint8_t *bitmap_;
};

} // namespace gandiva

#endif //GANDIVA_BITMAP_DEXVISITOR_H
