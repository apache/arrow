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

#include "arrow/util/delimiting.h"
#include "arrow/buffer.h"
#include "arrow/util/logging.h"

namespace arrow {

BoundaryFinder::~BoundaryFinder() {}

namespace {

Status StraddlingTooLarge() {
  return Status::Invalid(
      "straddling object straddles two block boundaries (try to increase block size?)");
}

class NewlineBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(util::string_view partial, util::string_view block,
                   int64_t* out_pos) override {
    auto pos = block.find_first_of(newline_delimiters);
    if (pos == util::string_view::npos) {
      *out_pos = kNoDelimiterFound;
    } else {
      auto end = block.find_first_not_of(newline_delimiters, pos);
      if (end == util::string_view::npos) {
        end = block.length();
      }
      *out_pos = static_cast<int64_t>(end);
    }
    return Status::OK();
  }

  Status FindLast(util::string_view block, int64_t* out_pos) override {
    auto pos = block.find_last_of(newline_delimiters);
    if (pos == util::string_view::npos) {
      *out_pos = kNoDelimiterFound;
    } else {
      auto end = block.find_first_not_of(newline_delimiters, pos);
      if (end == util::string_view::npos) {
        end = block.length();
      }
      *out_pos = static_cast<int64_t>(end);
    }
    return Status::OK();
  }

  Status FindNth(util::string_view partial, util::string_view block, int64_t count,
                 int64_t* out_pos, int64_t* num_found) override {
    DCHECK(partial.find_first_of(newline_delimiters) == util::string_view::npos);

    int64_t found = 0;
    int64_t pos = kNoDelimiterFound;

    auto cur_pos = block.find_first_of(newline_delimiters);
    while (cur_pos != util::string_view::npos) {
      if (block[cur_pos] == '\r' && cur_pos + 1 < block.length() &&
          block[cur_pos + 1] == '\n') {
        cur_pos += 2;
      } else {
        ++cur_pos;
      }

      pos = static_cast<int64_t>(cur_pos);
      if (++found >= count) {
        break;
      }

      cur_pos = block.find_first_of(newline_delimiters, cur_pos);
    }

    *out_pos = pos;
    *num_found = found;
    return Status::OK();
  }

 protected:
  static constexpr const char* newline_delimiters = "\r\n";
};

}  // namespace

std::shared_ptr<BoundaryFinder> MakeNewlineBoundaryFinder() {
  return std::make_shared<NewlineBoundaryFinder>();
}

Chunker::~Chunker() {}

Chunker::Chunker(std::shared_ptr<BoundaryFinder> delimiter)
    : boundary_finder_(delimiter) {}

Status Chunker::Process(std::shared_ptr<Buffer> block, std::shared_ptr<Buffer>* whole,
                        std::shared_ptr<Buffer>* partial) {
  int64_t last_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindLast(util::string_view(*block), &last_pos));
  if (last_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter found
    *whole = SliceBuffer(block, 0, 0);
    *partial = block;
    return Status::OK();
  } else {
    *whole = SliceBuffer(block, 0, last_pos);
    *partial = SliceBuffer(block, last_pos);
  }
  return Status::OK();
}

Status Chunker::ProcessWithPartial(std::shared_ptr<Buffer> partial,
                                   std::shared_ptr<Buffer> block,
                                   std::shared_ptr<Buffer>* completion,
                                   std::shared_ptr<Buffer>* rest) {
  if (partial->size() == 0) {
    // If partial is empty, don't bother looking for completion
    *completion = SliceBuffer(block, 0, 0);
    *rest = block;
    return Status::OK();
  }
  int64_t first_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindFirst(util::string_view(*partial),
                                            util::string_view(*block), &first_pos));
  if (first_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter in block => the current object is too large for block size
    return StraddlingTooLarge();
  } else {
    *completion = SliceBuffer(block, 0, first_pos);
    *rest = SliceBuffer(block, first_pos);
    return Status::OK();
  }
}

Status Chunker::ProcessFinal(std::shared_ptr<Buffer> partial,
                             std::shared_ptr<Buffer> block,
                             std::shared_ptr<Buffer>* completion,
                             std::shared_ptr<Buffer>* rest) {
  if (partial->size() == 0) {
    // If partial is empty, don't bother looking for completion
    *completion = SliceBuffer(block, 0, 0);
    *rest = block;
    return Status::OK();
  }
  int64_t first_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindFirst(util::string_view(*partial),
                                            util::string_view(*block), &first_pos));
  if (first_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter in block => it's entirely a completion of partial
    *completion = block;
    *rest = SliceBuffer(block, 0, 0);
  } else {
    *completion = SliceBuffer(block, 0, first_pos);
    *rest = SliceBuffer(block, first_pos);
  }
  return Status::OK();
}

Status Chunker::ProcessSkip(std::shared_ptr<Buffer> partial,
                            std::shared_ptr<Buffer> block, bool final, int64_t* count,
                            std::shared_ptr<Buffer>* rest) {
  DCHECK_GT(*count, 0);
  int64_t pos;
  int64_t num_found;
  ARROW_RETURN_NOT_OK(boundary_finder_->FindNth(
      util::string_view(*partial), util::string_view(*block), *count, &pos, &num_found));
  if (pos == BoundaryFinder::kNoDelimiterFound) {
    return StraddlingTooLarge();
  }
  if (ARROW_PREDICT_FALSE(final && *count > num_found && block->size() != pos)) {
    // Skip the last row in the final block which does not have a delimiter
    ++num_found;
    *rest = SliceBuffer(block, 0, 0);
  } else {
    *rest = SliceBuffer(block, pos);
  }
  *count -= num_found;
  return Status::OK();
}

}  // namespace arrow
