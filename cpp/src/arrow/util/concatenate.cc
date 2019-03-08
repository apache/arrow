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

  struct Bitmap {
    Bitmap() = default;
    Bitmap(const uint8_t* d, Range r) : data(d), range(r) {}
    explicit Bitmap(const std::shared_ptr<Buffer>& buffer, Range r = Range())
        : Bitmap(buffer ? buffer->data() : nullptr, r) {}

    const uint8_t* data;
    Range range;

    bool AllSet() const { return data == nullptr; }
  };

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    return ConcatenateBitmaps(Slices(Bitmaps(1), Ranges()), &out_.buffers[1]);
  }

  // handle numbers, decimal128, fixed_size_binary
  Status Visit(const FixedWidthType& fixed) {
    return arrow::Concatenate(Slices(Buffers(1), FixedWidthRanges(fixed)), pool_,
                              &out_.buffers[1]);
  }

  Status Visit(const BinaryType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(Buffers(1), &out_.buffers[1], &value_ranges));
    return arrow::Concatenate(Slices(Buffers(2), value_ranges), pool_, &out_.buffers[2]);
  }

  Status Visit(const ListType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(Buffers(1), &out_.buffers[1], &value_ranges));
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
    if (out_.null_count != 0) {
      RETURN_NOT_OK(ConcatenateBitmaps(Slices(Bitmaps(0), Ranges()), &out_.buffers[0]));
    }
    RETURN_NOT_OK(VisitTypeInline(*out_.type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  BufferVector Buffers(int index) {
    BufferVector buffers(in_size());
    for (int i = 0; i != in_size(); ++i) {
      buffers[i] = in_[i].buffers[index];
    }
    return buffers;
  }

  std::vector<Bitmap> Bitmaps(int index) {
    std::vector<Bitmap> bitmaps(in_size());
    for (int i = 0; i != in_size(); ++i) {
      bitmaps[i] = Bitmap(in_[i].buffers[index]);
    }
    return bitmaps;
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

  Bitmap Slice(Bitmap b, Range r) { return Bitmap(b.data, r); }

  template <typename Slicable>
  std::vector<Slicable> Slices(const std::vector<Slicable>& slicable,
                               const std::vector<Range>& ranges) {
    std::vector<Slicable> slices(in_size());
    for (int i = 0; i < in_size(); ++i) {
      slices[i] = Slice(slicable[i], ranges[i]);
    }
    return slices;
  }

  std::vector<Range> Ranges() {
    std::vector<Range> ranges(in_size());
    for (int i = 0; i < in_size(); ++i) {
      ranges[i] = in_range(i);
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

  Status ConcatenateBitmaps(const std::vector<Bitmap>& bitmaps,
                            std::shared_ptr<Buffer>* out) {
    RETURN_NOT_OK(AllocateBitmap(pool_, out_.length, out));
    uint8_t* bitmap_data = (*out)->mutable_data();
    int64_t bitmap_offset = 0;
    for (int i = 0; i < in_size(); ++i) {
      auto bitmap = bitmaps[i];
      if (bitmap.AllSet()) {
        BitUtil::SetBitsTo(bitmap_data, bitmap_offset, bitmap.range.length, true);
      } else {
        internal::CopyBitmap(bitmap.data, bitmap.range.offset, bitmap.range.length,
                             bitmap_data, bitmap_offset, false);
      }
      bitmap_offset += bitmap.range.length;
    }
    if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_.length % 8]) {
      bitmap_data[out_.length / 8] &= preceding_bits;
    }
    return Status::OK();
  }

  Status PutOffsets(const std::shared_ptr<Buffer>& src, Range range,
                    int32_t values_length, int32_t* dst_begin, Range* values_range) {
    auto src_begin = reinterpret_cast<const int32_t*>(src->data()) + range.offset;
    auto src_end = src_begin + range.length;
    auto first_offset = src_begin[0];
    values_range->offset = first_offset;
    values_range->length = *src_end - values_range->offset;
    if (values_length > std::numeric_limits<int32_t>::max() - values_range->length) {
      return Status::Invalid("offset overflow while concatenating arrays");
    }
    std::transform(src_begin, src_end, dst_begin,
                   [values_length, first_offset](int32_t offset) {
                     return offset - first_offset + values_length;
                   });
    return Status::OK();
  }

  Status ConcatenateOffsets(const BufferVector& buffers, std::shared_ptr<Buffer>* out,
                            std::vector<Range>* ranges) {
    RETURN_NOT_OK(AllocateBuffer(pool_, (out_.length + 1) * sizeof(int32_t), out));
    auto dst = reinterpret_cast<int32_t*>((*out)->mutable_data());
    int64_t elements_length = 0;
    int32_t values_length = 0;
    ranges->resize(in_size());
    for (int i = 0; i < in_size(); ++i) {
      RETURN_NOT_OK(PutOffsets(buffers[i], in_range(i), values_length,
                               &dst[elements_length], &ranges->at(i)));
      elements_length += in_length(i);
      values_length += static_cast<int32_t>(ranges->at(i).length);
    }
    dst[elements_length] = values_length;
    return Status::OK();
  }

  int in_size() const { return static_cast<int>(in_.size()); }
  int64_t in_offset(int i) const { return in_[i].offset; }
  int64_t in_length(int i) const { return in_[i].length; }
  Range in_range(int i) const { return Range(in_offset(i), in_length(i)); }

  const std::vector<ArrayData>& in_;
  MemoryPool* pool_;
  ArrayData out_;
};

Status Concatenate(const ArrayVector& arrays, MemoryPool* pool,
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
  ArrayData out_data;
  RETURN_NOT_OK(ConcatenateImpl(data, pool).Concatenate(&out_data));
  *out = MakeArray(std::make_shared<ArrayData>(std::move(out_data)));
  return Status::OK();
}

}  // namespace arrow
