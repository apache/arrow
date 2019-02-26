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

#include <memory>
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
      : in_(in),
        in_size_(static_cast<int>(in.size())),
        lengths_(in.size()),
        offsets_(in.size()),
        pool_(pool) {
    out_.type = in[0].type;
    for (int i = 0; i != in_size_; ++i) {
      lengths_[i] = in[i].length;
      offsets_[i] = in[i].offset;
      out_.length += lengths_[i];
      if (out_.null_count == kUnknownNullCount || in[i].null_count == kUnknownNullCount) {
        out_.null_count = kUnknownNullCount;
        continue;
      }
      out_.null_count += in[i].null_count;
    }
  }

  /// offset, length pair for representing a range of a buffer or array
  struct range {
    int32_t offset, length;

    range() : offset(-1), length(0) {}
    range(int32_t o, int32_t l) : offset(o), length(l) {}
    void widen_to_include(int32_t index) {
      if (length == 0) {
        offset = index;
        length = 1;
        return;
      }
      auto end = std::max(offset + length, index + 1);
      length = end - offset;
      offset = std::min(index, offset);
    }
  };

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(ConcatenateBitmaps(1, &values_buffer));
    out_.buffers.push_back(values_buffer);
    return Status::OK();
  }

  // handle numbers, decimal128, fixed_size_binary
  Status Visit(const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    const int byte_width = fixed.bit_width() / 8;
    std::shared_ptr<Buffer> values_buffer;
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      auto byte_length = byte_width * lengths_[i];
      auto byte_offset = byte_width * offsets_[i];
      values_slices[i] = SliceBuffer(in_[i].buffers[1], byte_offset, byte_length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &values_buffer));
    out_.buffers.push_back(values_buffer);
    return Status::OK();
  }

  Status Visit(const BinaryType&) {
    std::shared_ptr<Buffer> values_buffer, offset_buffer;
    std::vector<range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &offset_buffer, &value_ranges));
    out_.buffers.push_back(offset_buffer);
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      values_slices[i] =
          SliceBuffer(in_[i].buffers[2], value_ranges[i].offset, value_ranges[i].length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &values_buffer));
    out_.buffers.push_back(values_buffer);
    return Status::OK();
  }

  Status Visit(const ListType&) {
    std::shared_ptr<Buffer> offset_buffer;
    std::vector<range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &offset_buffer, &value_ranges));
    out_.buffers.push_back(offset_buffer);
    std::vector<ArrayData> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      values_slices[i] = SliceData(*in_[i].child_data[0], value_ranges[i].offset,
                                   value_ranges[i].length);
    }
    auto values = std::make_shared<ArrayData>();
    RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(values.get()));
    out_.child_data = {values};
    return Status::OK();
  }

  Status Visit(const StructType& s) {
    std::vector<ArrayData> values_slices(in_size_);
    for (int field_index = 0; field_index != s.num_children(); ++field_index) {
      for (int i = 0; i != in_size_; ++i) {
        values_slices[i] =
            SliceData(*in_[i].child_data[field_index], offsets_[i], lengths_[i]);
      }
      auto values = std::make_shared<ArrayData>();
      RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(values.get()));
      out_.child_data.push_back(values);
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& d) {
    std::vector<ArrayData> indices_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      indices_slices[i] = SliceData(in_[i], offsets_[i], lengths_[i]);
      indices_slices[i].type = d.index_type();
      // don't bother concatenating null bitmaps again
      indices_slices[i].null_count = 0;
      indices_slices[i].buffers[0] = nullptr;
    }
    ArrayData indices;
    RETURN_NOT_OK(ConcatenateImpl(indices_slices, pool_).Concatenate(&indices));
    out_.buffers.push_back(indices.buffers[1]);
    return Status::OK();
  }

  Status Visit(const UnionType& u) {
    // type_codes are an index into child_data
    std::vector<std::shared_ptr<Buffer>> codes_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      codes_slices[i] = SliceBuffer(in_[i].buffers[1], offsets_[i], lengths_[i]);
    }
    std::shared_ptr<Buffer> codes_buffer;
    RETURN_NOT_OK(arrow::Concatenate(codes_slices, pool_, &codes_buffer));
    if (u.mode() == UnionMode::SPARSE) {
      std::vector<ArrayData> values_slices(in_size_);
      for (int field_index = 0; field_index != u.num_children(); ++field_index) {
        for (int i = 0; i != in_size_; ++i) {
          values_slices[i] =
              SliceData(*in_[i].child_data[field_index], offsets_[i], lengths_[i]);
        }
        auto values = std::make_shared<ArrayData>();
        RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(values.get()));
        out_.child_data.push_back(values);
      }
      out_.buffers.push_back(nullptr);
      return Status::OK();
    }
    DCHECK_EQ(u.mode(), UnionMode::DENSE);

    // build mapping from (input, type_code)->the range of values actually referenced
    auto max_code = *std::max_element(u.type_codes().begin(), u.type_codes().end());
    std::vector<range> values_ranges((static_cast<int>(max_code) + 1) * in_size_);
    auto get_values_range = [&](int in_i, UnionArray::type_id_t code) -> range& {
      return values_ranges[in_i * in_size_ + code];
    };
    for (int i = 0; i != in_size_; ++i) {
      auto codes = in_[i].buffers[1]->data();
      auto offsets = reinterpret_cast<const int32_t*>(in_[i].buffers[2]->data());
      for (auto index = offsets_[i]; index != offsets_[i] + lengths_[i]; ++index) {
        get_values_range(i, codes[index]).widen_to_include(offsets[index]);
      }
    }

    // for each type_code, use the min/max offset as a slice range
    // and concatenate sliced data for that type_code
    out_.child_data.resize(static_cast<int>(max_code) + 1);
    for (auto code : u.type_codes()) {
      std::vector<ArrayData> values_slices(in_size_);
      for (int i = 0; i != in_size_; ++i) {
        auto values_range = get_values_range(i, code);
        values_slices[i] =
            SliceData(*in_[i].child_data[code], values_range.offset, values_range.length);
      }
      auto values = std::make_shared<ArrayData>();
      RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(values.get()));
      out_.child_data[code] = values;
    }

    // for each input array, adjust the offsets by the length of the slice
    // range for a given type code and the minimum offset in that input array,
    // so that offsets point into the concatenated values
    std::vector<int32_t> total_lengths(static_cast<int>(max_code) + 1, 0);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(AllocateBuffer(pool_, out_.length * sizeof(int32_t), &offsets_buffer));
    auto raw_offsets = reinterpret_cast<int32_t*>(offsets_buffer->mutable_data());
    for (int i = 0; i != in_size_; ++i) {
      auto codes = in_[i].buffers[1]->data();
      auto offsets = reinterpret_cast<const int32_t*>(in_[i].buffers[2]->data());
      for (auto index = offsets_[i]; index != offsets_[i] + lengths_[i]; ++index) {
        auto min_offset = get_values_range(i, codes[index]).offset;
        *raw_offsets++ = offsets[index] - min_offset + total_lengths[codes[index]];
      }
      for (auto code : u.type_codes()) {
        total_lengths[code] += get_values_range(i, code).length;
      }
    }
    out_.buffers.push_back(offsets_buffer);
    return Status::OK();
  }

  Status Visit(const ExtensionType&) {
    // XXX can we just concatenate their storage?
    return Status::NotImplemented("concatenation of extension arrays");
  }

  Status Concatenate(ArrayData* out) && {
    std::shared_ptr<Buffer> null_bitmap;
    if (out_.null_count != 0) {
      RETURN_NOT_OK(ConcatenateBitmaps(0, &null_bitmap));
    }
    out_.buffers = {null_bitmap};
    RETURN_NOT_OK(VisitTypeInline(*out_.type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  Status ConcatenateBitmaps(int index, std::shared_ptr<Buffer>* bitmap_buffer) {
    RETURN_NOT_OK(AllocateBitmap(pool_, out_.length, bitmap_buffer));
    uint8_t* bitmap_data = (*bitmap_buffer)->mutable_data();
    int64_t bitmap_offset = 0;
    for (int i = 0; i != in_size_; ++i) {
      if (auto bitmap = in_[i].buffers[0]) {
        internal::CopyBitmap(bitmap->data(), offsets_[i], lengths_[i], bitmap_data,
                             bitmap_offset);
      } else {
        BitUtil::SetBitsTo(bitmap_data, bitmap_offset, lengths_[i], true);
      }
      bitmap_offset += lengths_[i];
    }
    if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_.length % 8]) {
      bitmap_data[out_.length / 8] &= preceding_bits;
    }
    return Status::OK();
  }

  // FIXME the below assumes that the first offset in the inputs will always be 0, which
  // isn't necessarily correct. Accumulating first and last offsets will be necessary
  // because we need to slice the referenced data (child_data for lists, values_buffer for
  // strings)
  Status ConcatenateOffsets(int index, std::shared_ptr<Buffer>* offset_buffer,
                            std::vector<range>* ranges) {
    RETURN_NOT_OK(
        AllocateBuffer(pool_, (out_.length + 1) * sizeof(int32_t), offset_buffer));
    auto dst_offsets = reinterpret_cast<int32_t*>((*offset_buffer)->mutable_data());
    int32_t total_length = 0;
    ranges->resize(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      auto src_offsets_begin = in_[i].GetValues<int32_t>(index) + offsets_[i];
      auto src_offsets_end = src_offsets_begin + lengths_[i];
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
      dst_offsets += lengths_[i];
    }
    *dst_offsets = total_length;
    return Status::OK();
  }

  const std::vector<ArrayData>& in_;
  int in_size_;
  std::vector<int64_t> lengths_, offsets_;
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
