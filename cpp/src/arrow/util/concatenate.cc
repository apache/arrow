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

#include "arrow/util/concatenate.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

struct ConcatenateImpl {
  ConcatenateImpl(const std::vector<ArrayData>& in, MemoryPool* pool)
      : in_(in), pool_(pool) {
    out_.type = in[0].type;
    for (int i = 0; i < in_size(); ++i) {
      out_.length += in[i].length;
      if (out_.null_count == kUnknownNullCount || in[i].null_count == kUnknownNullCount) {
        out_.null_count = kUnknownNullCount;
        continue;
      }
      out_.null_count += in[i].null_count;
    }
    out_.buffers.resize(in[0].buffers.size());
    out_.child_data.resize(in[0].child_data.size());
    for (auto& data : out_.child_data) {
      data = std::make_shared<ArrayData>();
    }
  }

  /// offset, length pair for representing a Range of a buffer or array
  struct Range {
    int64_t offset, length;

    Range() : offset(-1), length(0) {}
    Range(int64_t o, int64_t l) : offset(o), length(l) {}
  };

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) { return ConcatenateBitmaps(1, &out_.buffers[1]); }

  // handle numbers, decimal128, fixed_size_binary
  Status Visit(const FixedWidthType& fixed) {
    return arrow::Concatenate(Slices(Buffers(1), FixedWidthRanges(fixed)), pool_,
                              &out_.buffers[1]);
  }

  Status Visit(const BinaryType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &out_.buffers[1], &value_ranges));
    return arrow::Concatenate(Slices(Buffers(2), value_ranges), pool_, &out_.buffers[2]);
  }

  Status Visit(const ListType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &out_.buffers[1], &value_ranges));
    return ConcatenateImpl(Slices(ChildData(0), value_ranges), pool_)
        .Concatenate(out_.child_data[0].get());
  }

  Status Visit(const StructType& s) {
    auto ranges = Ranges();
    for (int i = 0; i < s.num_children(); ++i) {
      RETURN_NOT_OK(ConcatenateImpl(Slices(ChildData(i), ranges), pool_)
                        .Concatenate(out_.child_data[i].get()));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& d) {
    auto fixed = internal::checked_cast<const FixedWidthType*>(d.index_type().get());
    return arrow::Concatenate(Slices(Buffers(1), FixedWidthRanges(*fixed)), pool_,
                              &out_.buffers[1]);
  }

  Status Visit(const UnionType& u) {
    return Status::NotImplemented("concatenation of ", u);
  }

  Status Visit(const ExtensionType& e) {
    // XXX can we just concatenate their storage?
    return Status::NotImplemented("concatenation of ", e);
  }

  Status Concatenate(ArrayData* out) && {
    std::shared_ptr<Buffer> null_bitmap;
    if (out_.null_count != 0) {
      RETURN_NOT_OK(ConcatenateBitmaps(0, &null_bitmap));
    }
    out_.buffers[0] = null_bitmap;
    RETURN_NOT_OK(VisitTypeInline(*out_.type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  std::vector<std::shared_ptr<Buffer>> Buffers(int index) {
    std::vector<std::shared_ptr<Buffer>> buffers(in_size());
    for (int i = 0; i != in_size(); ++i) {
      buffers[i] = in_[i].buffers[index];
    }
    return buffers;
  }

  std::vector<ArrayData> ChildData(int index) {
    std::vector<ArrayData> child_data(in_size());
    for (int i = 0; i != in_size(); ++i) {
      child_data[i] = *in_[i].child_data[index];
    }
    return child_data;
  }

  ArrayData Slice(const ArrayData& d, Range r) { return d.Slice(r.offset, r.length); }

  std::shared_ptr<Buffer> Slice(const std::shared_ptr<Buffer>& b, Range r) {
    return SliceBuffer(b, r.offset, r.length);
  }

  template <typename Slicable>
  std::vector<Slicable> Slices(const std::vector<Slicable>& slicable,
                               const std::vector<Range>& ranges) {
    std::vector<Slicable> values_slices(in_size());
    for (int i = 0; i < in_size(); ++i) {
      values_slices[i] = Slice(slicable[i], ranges[i]);
    }
    return values_slices;
  }

  std::vector<Range> Ranges() {
    std::vector<Range> ranges(in_size());
    for (int i = 0; i < in_size(); ++i) {
      ranges[i] = Range(in_offset(i), in_length(i));
    }
    return ranges;
  }

  std::vector<Range> FixedWidthRanges(const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    auto byte_width = fixed.bit_width() / 8;
    auto ranges = Ranges();
    for (Range& range : ranges) {
      range.offset *= byte_width;
      range.length *= byte_width;
    }
    return ranges;
  }

  Status ConcatenateBitmaps(int index, std::shared_ptr<Buffer>* bitmap_buffer) {
    RETURN_NOT_OK(AllocateBitmap(pool_, out_.length, bitmap_buffer));
    uint8_t* bitmap_data = (*bitmap_buffer)->mutable_data();
    int64_t bitmap_offset = 0;
    for (int i = 0; i < in_size(); ++i) {
      if (auto bitmap = in_[i].buffers[index]) {
        internal::CopyBitmap(bitmap->data(), in_offset(i), in_length(i), bitmap_data,
                             bitmap_offset, false);
      } else {
        BitUtil::SetBitsTo(bitmap_data, bitmap_offset, in_length(i), true);
      }
      bitmap_offset += in_length(i);
    }
    if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_.length % 8]) {
      bitmap_data[out_.length / 8] &= preceding_bits;
    }
    return Status::OK();
  }

  Status ConcatenateOffsets(int index, std::shared_ptr<Buffer>* offset_buffer,
                            std::vector<Range>* ranges) {
    RETURN_NOT_OK(
        AllocateBuffer(pool_, (out_.length + 1) * sizeof(int32_t), offset_buffer));
    auto dst_offsets = reinterpret_cast<int32_t*>((*offset_buffer)->mutable_data());
    int32_t total_length = 0;
    ranges->resize(in_size());
    for (int i = 0; i < in_size(); ++i) {
      auto src_offsets_begin = in_[i].GetValues<int32_t>(index);
      auto src_offsets_end = src_offsets_begin + in_length(i);
      auto first_offset = src_offsets_begin[0];
      auto length = *src_offsets_end - first_offset;
      ranges->at(i).offset = first_offset;
      ranges->at(i).length = length;
      if (total_length > std::numeric_limits<int32_t>::max() - length) {
        return Status::Invalid("offset overflow while concatenating arrays");
      }
      std::transform(src_offsets_begin, src_offsets_end, dst_offsets,
                     [total_length, first_offset](int32_t offset) {
                       return offset - first_offset + total_length;
                     });
      total_length += length;
      dst_offsets += in_length(i);
    }
    *dst_offsets = total_length;
    return Status::OK();
  }

  int in_size() const { return static_cast<int>(in_.size()); }
  int64_t in_offset(int i) const { return in_[i].offset; }
  int64_t in_length(int i) const { return in_[i].length; }

  const std::vector<ArrayData>& in_;
  MemoryPool* pool_;
  ArrayData out_;
};

Status Concatenate(const std::vector<std::shared_ptr<Array>>& arrays, MemoryPool* pool,
                   std::shared_ptr<Array>* out) {
  DCHECK_GT(arrays.size(), 0);
  std::vector<ArrayData> data(arrays.size());
  for (std::size_t i = 0; i < arrays.size(); ++i) {
    if (!arrays[i]->type()->Equals(*arrays[0]->type())) {
      return Status::Invalid("arrays to be concatentated must be identically typed, but ",
                             *arrays[0]->type(), " and ", *arrays[i]->type(),
                             " were encountered.");
    }
    data[i] = ArrayData(*arrays[i]->data());
  }
  auto out_data = std::make_shared<ArrayData>();
  RETURN_NOT_OK(ConcatenateImpl(data, pool).Concatenate(out_data.get()));
  *out = MakeArray(std::move(out_data));
  return Status::OK();
}

}  // namespace arrow
