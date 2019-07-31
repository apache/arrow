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

#include "arrow/array/concatenate.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

/// offset, length pair for representing a Range of a buffer or array
struct Range {
  int64_t offset, length;

  Range() : offset(-1), length(0) {}
  Range(int64_t o, int64_t l) : offset(o), length(l) {}
};

/// non-owning view into a range of bits
struct Bitmap {
  Bitmap() = default;
  Bitmap(const uint8_t* d, Range r) : data(d), range(r) {}
  explicit Bitmap(const std::shared_ptr<Buffer>& buffer, Range r)
      : Bitmap(buffer ? buffer->data() : nullptr, r) {}

  const uint8_t* data;
  Range range;

  bool AllSet() const { return data == nullptr; }
};

// Allocate a buffer and concatenate bitmaps into it.
static Status ConcatenateBitmaps(const std::vector<Bitmap>& bitmaps, MemoryPool* pool,
                                 std::shared_ptr<Buffer>* out) {
  int64_t out_length = 0;
  for (size_t i = 0; i < bitmaps.size(); ++i) {
    out_length += bitmaps[i].range.length;
  }
  RETURN_NOT_OK(AllocateBitmap(pool, out_length, out));
  uint8_t* dst = (*out)->mutable_data();

  int64_t bitmap_offset = 0;
  for (size_t i = 0; i < bitmaps.size(); ++i) {
    auto bitmap = bitmaps[i];
    if (bitmap.AllSet()) {
      BitUtil::SetBitsTo(dst, bitmap_offset, bitmap.range.length, true);
    } else {
      internal::CopyBitmap(bitmap.data, bitmap.range.offset, bitmap.range.length, dst,
                           bitmap_offset, false);
    }
    bitmap_offset += bitmap.range.length;
  }

  // finally (if applicable) zero out any trailing bits
  if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_length % 8]) {
    dst[out_length / 8] &= preceding_bits;
  }
  return Status::OK();
}

// Write offsets in src into dst, adjusting them such that first_offset
// will be the first offset written.
template <typename Offset>
static Status PutOffsets(const std::shared_ptr<Buffer>& src, Offset first_offset,
                         Offset* dst, Range* values_range);

// Concatenate buffers holding offsets into a single buffer of offsets,
// also computing the ranges of values spanned by each buffer of offsets.
template <typename Offset>
static Status ConcatenateOffsets(const BufferVector& buffers, MemoryPool* pool,
                                 std::shared_ptr<Buffer>* out,
                                 std::vector<Range>* values_ranges) {
  values_ranges->resize(buffers.size());

  // allocate output buffer
  int64_t out_length = 0;
  for (size_t i = 0; i < buffers.size(); ++i) {
    out_length += buffers[i]->size() / sizeof(Offset);
  }
  RETURN_NOT_OK(AllocateBuffer(pool, (out_length + 1) * sizeof(Offset), out));
  auto dst = reinterpret_cast<Offset*>((*out)->mutable_data());

  int64_t elements_length = 0;
  Offset values_length = 0;
  for (size_t i = 0; i < buffers.size(); ++i) {
    // the first offset from buffers[i] will be adjusted to values_length
    // (the cumulative length of values spanned by offsets in previous buffers)
    RETURN_NOT_OK(PutOffsets<Offset>(buffers[i], values_length, &dst[elements_length],
                                     &values_ranges->at(i)));
    elements_length += buffers[i]->size() / sizeof(Offset);
    values_length += static_cast<Offset>(values_ranges->at(i).length);
  }

  // the final element in dst is the length of all values spanned by the offsets
  dst[out_length] = values_length;
  return Status::OK();
}

template <typename Offset>
static Status PutOffsets(const std::shared_ptr<Buffer>& src, Offset first_offset,
                         Offset* dst, Range* values_range) {
  // Get the range of offsets to transfer from src
  auto src_begin = reinterpret_cast<const Offset*>(src->data());
  auto src_end = reinterpret_cast<const Offset*>(src->data() + src->size());

  // Compute the range of values which is spanned by this range of offsets
  values_range->offset = src_begin[0];
  values_range->length = *src_end - values_range->offset;
  if (first_offset > std::numeric_limits<Offset>::max() - values_range->length) {
    return Status::Invalid("offset overflow while concatenating arrays");
  }

  // Write offsets into dst, ensuring that the first offset written is
  // first_offset
  auto adjustment = first_offset - src_begin[0];
  std::transform(src_begin, src_end, dst,
                 [adjustment](Offset offset) { return offset + adjustment; });
  return Status::OK();
}

class ConcatenateImpl {
 public:
  ConcatenateImpl(const std::vector<ArrayData>& in, MemoryPool* pool)
      : in_(in), pool_(pool) {
    out_.type = in[0].type;
    for (size_t i = 0; i < in_.size(); ++i) {
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

  Status Concatenate(ArrayData* out) && {
    if (out_.null_count != 0) {
      RETURN_NOT_OK(ConcatenateBitmaps(Bitmaps(0), pool_, &out_.buffers[0]));
    }
    RETURN_NOT_OK(VisitTypeInline(*out_.type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    return ConcatenateBitmaps(Bitmaps(1), pool_, &out_.buffers[1]);
  }

  Status Visit(const FixedWidthType& fixed) {
    // handles numbers, decimal128, fixed_size_binary
    return ConcatenateBuffers(Buffers(1, fixed), pool_, &out_.buffers[1]);
  }

  Status Visit(const BinaryType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets<int32_t>(Buffers(1, sizeof(int32_t)), pool_,
                                              &out_.buffers[1], &value_ranges));
    return ConcatenateBuffers(Buffers(2, value_ranges), pool_, &out_.buffers[2]);
  }

  Status Visit(const LargeBinaryType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets<int64_t>(Buffers(1, sizeof(int64_t)), pool_,
                                              &out_.buffers[1], &value_ranges));
    return ConcatenateBuffers(Buffers(2, value_ranges), pool_, &out_.buffers[2]);
  }

  Status Visit(const ListType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets<int32_t>(Buffers(1, sizeof(int32_t)), pool_,
                                              &out_.buffers[1], &value_ranges));
    return ConcatenateImpl(ChildData(0, value_ranges), pool_)
        .Concatenate(out_.child_data[0].get());
  }

  Status Visit(const LargeListType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets<int64_t>(Buffers(1, sizeof(int64_t)), pool_,
                                              &out_.buffers[1], &value_ranges));
    return ConcatenateImpl(ChildData(0, value_ranges), pool_)
        .Concatenate(out_.child_data[0].get());
  }

  Status Visit(const FixedSizeListType&) {
    return ConcatenateImpl(ChildData(0), pool_).Concatenate(out_.child_data[0].get());
  }

  Status Visit(const StructType& s) {
    for (int i = 0; i < s.num_children(); ++i) {
      RETURN_NOT_OK(
          ConcatenateImpl(ChildData(i), pool_).Concatenate(out_.child_data[i].get()));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& d) {
    auto fixed = internal::checked_cast<const FixedWidthType*>(d.index_type().get());

    // Two cases: all the dictionaries are the same, or unification is
    // required
    bool dictionaries_same = true;
    const Array& dictionary0 = *in_[0].dictionary;
    for (size_t i = 1; i < in_.size(); ++i) {
      if (!in_[i].dictionary->Equals(dictionary0)) {
        dictionaries_same = false;
        break;
      }
    }

    if (dictionaries_same) {
      out_.dictionary = in_[0].dictionary;
      return ConcatenateBuffers(Buffers(1, *fixed), pool_, &out_.buffers[1]);
    } else {
      return Status::NotImplemented("Concat with dictionary unification NYI");
    }
  }

  Status Visit(const UnionType& u) {
    return Status::NotImplemented("concatenation of ", u);
  }

  Status Visit(const ExtensionType& e) {
    // XXX can we just concatenate their storage?
    return Status::NotImplemented("concatenation of ", e);
  }

 private:
  // Gather the index-th buffer of each input into a vector.
  // Bytes are sliced with that input's offset and length.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  BufferVector Buffers(size_t index) {
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (const ArrayData& array_data : in_) {
      const auto& buffer = array_data.buffers[index];
      if (buffer != nullptr) {
        buffers.push_back(SliceBuffer(buffer, array_data.offset, array_data.length));
      }
    }
    return buffers;
  }

  // Gather the index-th buffer of each input into a vector.
  // Bytes are sliced with the explicitly passed ranges.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  BufferVector Buffers(size_t index, const std::vector<Range>& ranges) {
    DCHECK_EQ(in_.size(), ranges.size());
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      const auto& buffer = in_[i].buffers[index];
      if (buffer != nullptr) {
        buffers.push_back(SliceBuffer(buffer, ranges[i].offset, ranges[i].length));
      } else {
        DCHECK_EQ(ranges[i].length, 0);
      }
    }
    return buffers;
  }

  // Gather the index-th buffer of each input into a vector.
  // Buffers are assumed to contain elements of the given byte_width,
  // those elements are sliced with that input's offset and length.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  BufferVector Buffers(size_t index, int byte_width) {
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (const ArrayData& array_data : in_) {
      const auto& buffer = array_data.buffers[index];
      if (buffer != nullptr) {
        buffers.push_back(SliceBuffer(buffer, array_data.offset * byte_width,
                                      array_data.length * byte_width));
      }
    }
    return buffers;
  }

  // Gather the index-th buffer of each input into a vector.
  // Buffers are assumed to contain elements of fixed.bit_width(),
  // those elements are sliced with that input's offset and length.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  BufferVector Buffers(size_t index, const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    return Buffers(index, fixed.bit_width() / 8);
  }

  // Gather the index-th buffer of each input as a Bitmap
  // into a vector of Bitmaps.
  std::vector<Bitmap> Bitmaps(size_t index) {
    std::vector<Bitmap> bitmaps(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      Range range(in_[i].offset, in_[i].length);
      bitmaps[i] = Bitmap(in_[i].buffers[index], range);
    }
    return bitmaps;
  }

  // Gather the index-th child_data of each input into a vector.
  // Elements are sliced with that input's offset and length.
  std::vector<ArrayData> ChildData(size_t index) {
    std::vector<ArrayData> child_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      child_data[i] = in_[i].child_data[index]->Slice(in_[i].offset, in_[i].length);
    }
    return child_data;
  }

  // Gather the index-th child_data of each input into a vector.
  // Elements are sliced with the explicitly passed ranges.
  std::vector<ArrayData> ChildData(size_t index, const std::vector<Range>& ranges) {
    DCHECK_EQ(in_.size(), ranges.size());
    std::vector<ArrayData> child_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      child_data[i] = in_[i].child_data[index]->Slice(ranges[i].offset, ranges[i].length);
    }
    return child_data;
  }

  const std::vector<ArrayData>& in_;
  MemoryPool* pool_;
  ArrayData out_;
};

Status Concatenate(const ArrayVector& arrays, MemoryPool* pool,
                   std::shared_ptr<Array>* out) {
  if (arrays.size() == 0) {
    return Status::Invalid("Must pass at least one array");
  }

  // gather ArrayData of input arrays
  std::vector<ArrayData> data(arrays.size());
  for (size_t i = 0; i < arrays.size(); ++i) {
    if (!arrays[i]->type()->Equals(*arrays[0]->type())) {
      return Status::Invalid("arrays to be concatenated must be identically typed, but ",
                             *arrays[0]->type(), " and ", *arrays[i]->type(),
                             " were encountered.");
    }
    data[i] = *arrays[i]->data();
  }

  ArrayData out_data;
  RETURN_NOT_OK(ConcatenateImpl(data, pool).Concatenate(&out_data));
  *out = MakeArray(std::make_shared<ArrayData>(std::move(out_data)));
  return Status::OK();
}

}  // namespace arrow
