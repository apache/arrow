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

static inline ArrayData SliceData(const ArrayData& data, int64_t offset, int64_t length) {
  DCHECK_LE(offset, data.length);
  length = std::min(data.length - offset, length);
  offset += data.offset;

  auto copy = data;
  copy.length = length;
  copy.offset = offset;
  copy.null_count = data.null_count != 0 ? kUnknownNullCount : 0;
  return copy;
}

struct ConcatenateImpl {
  ConcatenateImpl(const std::vector<ArrayData>& in, MemoryPool* pool)
      : in_(in), pool_(pool) {
    out_.type = in[0].type;
    for (int i = 0; i != in_size(); ++i) {
      out_.length += in[i].length;
      if (out_.null_count == kUnknownNullCount || in[i].null_count == kUnknownNullCount) {
        out_.null_count = kUnknownNullCount;
        continue;
      }
      out_.null_count += in[i].null_count;
    }
    out_.buffers.resize(in[0].buffers.size());
    out_.child_data.resize(in[0].child_data.size());
  }

  /// offset, length pair for representing a Range of a buffer or array
  struct Range {
    int32_t offset, length;

    Range() : offset(-1), length(0) {}
    Range(int32_t o, int32_t l) : offset(o), length(l) {}
  };

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(ConcatenateBitmaps(1, &out_.buffers[1]));
    return Status::OK();
  }

  // handle numbers, decimal128, fixed_size_binary
  Status Visit(const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    const int byte_width = fixed.bit_width() / 8;
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size());
    for (int i = 0; i != in_size(); ++i) {
      auto byte_length = byte_width * in_length(i);
      auto byte_offset = byte_width * in_offset(i);
      values_slices[i] = SliceBuffer(in_[i].buffers[1], byte_offset, byte_length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &out_.buffers[1]));
    return Status::OK();
  }

  Status Visit(const BinaryType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &out_.buffers[1], &value_ranges));
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size());
    for (int i = 0; i != in_size(); ++i) {
      values_slices[i] =
          SliceBuffer(in_[i].buffers[2], value_ranges[i].offset, value_ranges[i].length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &out_.buffers[2]));
    return Status::OK();
  }

  Status Visit(const ListType&) {
    std::vector<Range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &out_.buffers[1], &value_ranges));
    std::vector<ArrayData> values_slices(in_size());
    for (int i = 0; i != in_size(); ++i) {
      values_slices[i] = SliceData(*in_[i].child_data[0], value_ranges[i].offset,
                                   value_ranges[i].length);
    }
    out_.child_data[0] = std::make_shared<ArrayData>();
    RETURN_NOT_OK(
        ConcatenateImpl(values_slices, pool_).Concatenate(out_.child_data[0].get()));
    return Status::OK();
  }

  Status Visit(const StructType& s) {
    std::vector<ArrayData> values_slices(in_size());
    for (int field_index = 0; field_index != s.num_children(); ++field_index) {
      for (int i = 0; i != in_size(); ++i) {
        values_slices[i] =
            SliceData(*in_[i].child_data[field_index], in_offset(i), in_length(i));
      }
      out_.child_data[field_index] = std::make_shared<ArrayData>();
      RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_)
                        .Concatenate(out_.child_data[field_index].get()));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& d) {
    std::vector<ArrayData> indices_slices(in_size());
    for (int i = 0; i != in_size(); ++i) {
      indices_slices[i] = ArrayData(in_[i]);
      indices_slices[i].type = d.index_type();
      // don't bother concatenating null bitmaps again
      indices_slices[i].null_count = 0;
      indices_slices[i].buffers[0] = nullptr;
    }
    ArrayData indices;
    RETURN_NOT_OK(ConcatenateImpl(indices_slices, pool_).Concatenate(&indices));
    out_.buffers[1] = indices.buffers[1];
    return Status::OK();
  }

  Status Visit(const UnionType& u) {
    // type_codes are an index into child_data
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

  Status ConcatenateBitmaps(int index, std::shared_ptr<Buffer>* bitmap_buffer) {
    RETURN_NOT_OK(AllocateBitmap(pool_, out_.length, bitmap_buffer));
    uint8_t* bitmap_data = (*bitmap_buffer)->mutable_data();
    int64_t bitmap_offset = 0;
    for (int i = 0; i != in_size(); ++i) {
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
    for (int i = 0; i != in_size(); ++i) {
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
  for (std::size_t i = 0; i != arrays.size(); ++i) {
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
