// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/type.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"

namespace parquet::variant::internal {

// Builds binary-view columns from Variant bytes encoded directly into a shared arena.
// Arrow's BinaryViewBuilder accepts complete values and copies them into its private
// StringHeapBuilder, so it cannot expose the writable arena used by nested Variant
// encoders or roll back a partially committed row. Keeping bytes and views separate lets
// callers write each value once, create its view afterward, discard inlined bytes, and
// roll back both buffers together.
class BinaryViewColumnBuilder {
 public:
  struct Mark {
    int64_t bytes_length = 0;
    int64_t views_length = 0;
    bool has_out_of_line_data = false;
  };

  explicit BinaryViewColumnBuilder(::arrow::MemoryPool* pool)
      : bytes_(pool), views_(pool) {}

  [[nodiscard]] Mark mark() const {
    return Mark{.bytes_length = bytes_.length(),
                .views_length = views_.length(),
                .has_out_of_line_data = has_out_of_line_data_};
  }

  void Rollback(const Mark& mark) {
    bytes_.Rewind(mark.bytes_length);
    views_.bytes_builder()->Rewind(mark.views_length *
                                   sizeof(::arrow::BinaryViewType::c_type));
    has_out_of_line_data_ = mark.has_out_of_line_data;
  }

  int64_t length() const { return views_.length(); }

  std::string_view SliceFrom(int64_t start) const {
    DCHECK_LE(start, bytes_.length());
    const auto size = bytes_.length() - start;
    DCHECK_GE(size, 0);
    if (size == 0) {
      return std::string_view{};
    }
    return std::string_view{reinterpret_cast<const char*>(bytes_.data() + start),
                            static_cast<size_t>(size)};
  }

  void AppendView(int64_t start) {
    const auto slice = SliceFrom(start);
    if (slice.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      throw ParquetException("Binary view value is too large");
    }
    PARQUET_THROW_NOT_OK(views_.Reserve(1));
    if (slice.size() <= ::arrow::BinaryViewType::kInlineSize) {
      views_.UnsafeAppend(::arrow::util::ToInlineBinaryView(slice));
      bytes_.Rewind(start);
      return;
    }
    if (start > std::numeric_limits<int32_t>::max()) {
      throw ParquetException("Binary view offset is too large");
    }
    has_out_of_line_data_ = true;
    views_.UnsafeAppend(::arrow::util::ToNonInlineBinaryView(
        slice.data(), static_cast<int32_t>(slice.size()),
        /*buffer_index=*/0, static_cast<int32_t>(start)));
  }

  void AppendEmptyValue() {
    PARQUET_THROW_NOT_OK(views_.Reserve(1));
    views_.UnsafeAppend(::arrow::BinaryViewType::c_type{});
  }

  ::arrow::BufferBuilder& bytes_builder() { return bytes_; }

  std::shared_ptr<::arrow::BinaryViewArray> Finish(
      std::shared_ptr<::arrow::Buffer> null_bitmap = nullptr, int64_t null_count = 0) {
    const auto array_length = views_.length();
    std::shared_ptr<::arrow::Buffer> views_buffer;
    PARQUET_THROW_NOT_OK(views_.Finish(&views_buffer));

    PARQUET_ASSIGN_OR_THROW(auto data_buffer, bytes_.Finish());
    ::arrow::BufferVector data_buffers;
    if (has_out_of_line_data_) {
      data_buffers.push_back(std::move(data_buffer));
    }
    return std::make_shared<::arrow::BinaryViewArray>(
        ::arrow::binary_view(), array_length, std::move(views_buffer),
        std::move(data_buffers), std::move(null_bitmap), null_count);
  }

 private:
  ::arrow::BufferBuilder bytes_;
  ::arrow::TypedBufferBuilder<::arrow::BinaryViewType::c_type> views_;
  bool has_out_of_line_data_ = false;
};

}  // namespace parquet::variant::internal
