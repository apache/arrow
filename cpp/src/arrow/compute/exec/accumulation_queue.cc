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

#include "arrow/compute/exec/accumulation_queue.h"

#include <iterator>

namespace arrow {
namespace util {
using arrow::compute::ExecBatch;
AccumulationQueue::AccumulationQueue(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  this->row_count_ = that.row_count_;
  that.Clear();
}

AccumulationQueue& AccumulationQueue::operator=(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  this->row_count_ = that.row_count_;
  that.Clear();
  return *this;
}

void AccumulationQueue::Concatenate(AccumulationQueue&& that) {
  this->batches_.reserve(this->batches_.size() + that.batches_.size());
  std::move(that.batches_.begin(), that.batches_.end(),
            std::back_inserter(this->batches_));
  this->row_count_ += that.row_count_;
  that.Clear();
}

void AccumulationQueue::InsertBatch(ExecBatch batch) {
  row_count_ += batch.length;
  batches_.emplace_back(std::move(batch));
}

void AccumulationQueue::Clear() {
  row_count_ = 0;
  batches_.clear();
}

ExecBatch& AccumulationQueue::operator[](size_t i) { return batches_[i]; }
}  // namespace util
}  // namespace arrow
