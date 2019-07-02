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
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

/// \brief Compare two arrays, returning an edit script which expresses the difference
/// between them
///
/// An edit script is an array of struct(insert: bool, run_length: uint64_t).
/// Each element of "insert" determines whether an element was inserted into (true)
/// or deleted from (false) base. Each insertion or deletion is followed by a run of
/// elements which are unchanged from base to target; the length of this run is stored
/// in "run_length".
///
/// For example for base "hlloo" and target "hello", the edit script would be
/// [
///   {"insert": false, "run_length": 1}, // leading run of length 1 ("h")
///   {"insert": true, "run_length": 3}, // insert("e") then a run of length 3 ("llo")
///   {"insert": false, "run_length": 0} // delete("o") then an empty run
/// ]
///
/// Diffing arrays containing nulls is not currently supported.
///
/// \param[in] base baseline for comparison
/// \param[in] target an array of identical type to base whose elements differ from base's
/// \param[in] pool memory to store the result will be allocated from this memory pool
/// \param[out] out an edit script array which can be applied to base to produce target
/// \return Status
ARROW_EXPORT
Status Diff(const Array& base, const Array& target, MemoryPool* pool,
            std::shared_ptr<Array>* out);

// XXX would it be less confusing to switch vocabulary to actual/expected?

/// \brief visitor interface for easy traversal of an edit script
///
/// Implement whichever methods correspond to edits you are interested in then
/// call Visit on a valid edits array
class ARROW_EXPORT DiffVisitor {
 public:
  virtual ~DiffVisitor() = default;

  /// \brief called on visitation of insertion target[target_index]
  virtual Status Insert(int64_t target_index) { return Status::OK(); }

  /// \brief called on visitation of deletion of base[base_index]
  virtual Status Delete(int64_t base_index) { return Status::OK(); }

  /// \brief called on visitation of length identical elements
  virtual Status Run(int64_t length) { return Status::OK(); }

  /// \brief visit each insertion, deletion, or run of edits
  Status Visit(const Array& edits);
};

}  // namespace arrow
