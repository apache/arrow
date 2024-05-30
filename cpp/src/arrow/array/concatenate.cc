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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_run_end.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/slice_util_internal.h"
#include "arrow/visit_data_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::SafeSignedAdd;

namespace {
/// offset, length pair for representing a Range of a buffer or array
struct Range {
  int64_t offset = -1, length = 0;

  Range() = default;
  Range(int64_t o, int64_t l) : offset(o), length(l) {}
};

/// non-owning view into a range of bits
struct Bitmap {
  Bitmap() = default;
  Bitmap(const uint8_t* d, Range r) : data(d), range(r) {}
  explicit Bitmap(const std::shared_ptr<Buffer>& buffer, Range r)
      : Bitmap(buffer ? buffer->data() : nullptr, r) {}

  const uint8_t* data = nullptr;
  Range range;

  bool AllSet() const { return data == nullptr; }
};

// Allocate a buffer and concatenate bitmaps into it.
Status ConcatenateBitmaps(const std::vector<Bitmap>& bitmaps, MemoryPool* pool,
                          std::shared_ptr<Buffer>* out) {
  int64_t out_length = 0;
  for (const auto& bitmap : bitmaps) {
    if (internal::AddWithOverflow(out_length, bitmap.range.length, &out_length)) {
      return Status::Invalid("Length overflow when concatenating arrays");
    }
  }
  ARROW_ASSIGN_OR_RAISE(*out, AllocateBitmap(out_length, pool));
  uint8_t* dst = (*out)->mutable_data();

  int64_t bitmap_offset = 0;
  for (auto bitmap : bitmaps) {
    if (bitmap.AllSet()) {
      bit_util::SetBitsTo(dst, bitmap_offset, bitmap.range.length, true);
    } else {
      internal::CopyBitmap(bitmap.data, bitmap.range.offset, bitmap.range.length, dst,
                           bitmap_offset);
    }
    bitmap_offset += bitmap.range.length;
  }

  return Status::OK();
}

int64_t SumBufferSizesInBytes(const BufferVector& buffers) {
  int64_t size = 0;
  for (const auto& buffer : buffers) {
    size += buffer->size();
  }
  return size;
}

// Write offsets in src into dst, adjusting them such that first_offset
// will be the first offset written.
template <typename Offset>
Status PutOffsets(const Buffer& src, Offset first_offset, Offset* dst,
                  Range* values_range);

// Concatenate buffers holding offsets into a single buffer of offsets,
// also computing the ranges of values spanned by each buffer of offsets.
template <typename Offset>
Status ConcatenateOffsets(const BufferVector& buffers, MemoryPool* pool,
                          std::shared_ptr<Buffer>* out,
                          std::vector<Range>* values_ranges) {
  values_ranges->resize(buffers.size());

  // allocate output buffer
  const int64_t out_size_in_bytes = SumBufferSizesInBytes(buffers);
  ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer(sizeof(Offset) + out_size_in_bytes, pool));
  auto* out_data = (*out)->mutable_data_as<Offset>();

  int64_t elements_length = 0;
  Offset values_length = 0;
  for (size_t i = 0; i < buffers.size(); ++i) {
    // the first offset from buffers[i] will be adjusted to values_length
    // (the cumulative length of values spanned by offsets in previous buffers)
    RETURN_NOT_OK(PutOffsets<Offset>(*buffers[i], values_length,
                                     out_data + elements_length, &(*values_ranges)[i]));
    elements_length += buffers[i]->size() / sizeof(Offset);
    values_length += static_cast<Offset>((*values_ranges)[i].length);
  }

  // the final element in out_data is the length of all values spanned by the offsets
  out_data[out_size_in_bytes / sizeof(Offset)] = values_length;
  return Status::OK();
}

template <typename Offset>
Status PutOffsets(const Buffer& src, Offset first_offset, Offset* dst,
                  Range* values_range) {
  if (src.size() == 0) {
    // It's allowed to have an empty offsets buffer for a 0-length array
    // (see Array::Validate)
    values_range->offset = 0;
    values_range->length = 0;
    return Status::OK();
  }

  // Get the range of offsets to transfer from src
  auto src_begin = src.data_as<Offset>();
  auto src_end = reinterpret_cast<const Offset*>(src.data() + src.size());

  // Compute the range of values which is spanned by this range of offsets
  values_range->offset = src_begin[0];
  values_range->length = *src_end - values_range->offset;
  if (first_offset > std::numeric_limits<Offset>::max() - values_range->length) {
    return Status::Invalid("offset overflow while concatenating arrays");
  }

  // Write offsets into dst, ensuring that the first offset written is
  // first_offset
  auto displacement = first_offset - src_begin[0];
  // NOTE: Concatenate can be called during IPC reads to append delta dictionaries.
  // Avoid UB on non-validated input by doing the addition in the unsigned domain.
  // (the result can later be validated using Array::ValidateFull)
  std::transform(src_begin, src_end, dst, [displacement](Offset offset) {
    return SafeSignedAdd(offset, displacement);
  });
  return Status::OK();
}

template <typename offset_type>
Status PutListViewOffsets(const ArrayData& input, offset_type* sizes, const Buffer& src,
                          offset_type displacement, offset_type* dst);

// Concatenate buffers holding list-view offsets into a single buffer of offsets
//
// value_ranges contains the relevant ranges of values in the child array actually
// referenced to by the views. Most commonly, these ranges will start from 0,
// but when that is not the case, we need to adjust the displacement of offsets.
// The concatenated child array does not contain values from the beginning
// if they are not referenced to by any view.
//
// The child arrays and the sizes buffer are used to ensure we can trust the offsets in
// offset_buffers to be within the valid range.
//
// This function also mutates sizes so that null list-view entries have size 0.
//
// \param[in] in The child arrays
// \param[in,out] sizes The concatenated sizes buffer
template <typename offset_type>
Status ConcatenateListViewOffsets(const ArrayDataVector& in, offset_type* sizes,
                                  const BufferVector& offset_buffers,
                                  const std::vector<Range>& value_ranges,
                                  MemoryPool* pool, std::shared_ptr<Buffer>* out) {
  DCHECK_EQ(offset_buffers.size(), value_ranges.size());

  // Allocate resulting offsets buffer and initialize it with zeros
  const int64_t out_size_in_bytes = SumBufferSizesInBytes(offset_buffers);
  ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer(out_size_in_bytes, pool));
  memset((*out)->mutable_data(), 0, static_cast<size_t>((*out)->size()));

  auto* out_offsets = (*out)->mutable_data_as<offset_type>();

  int64_t num_child_values = 0;
  int64_t elements_length = 0;
  for (size_t i = 0; i < offset_buffers.size(); ++i) {
    const auto displacement =
        static_cast<offset_type>(num_child_values - value_ranges[i].offset);
    RETURN_NOT_OK(PutListViewOffsets(*in[i], /*sizes=*/sizes + elements_length,
                                     /*src=*/*offset_buffers[i], displacement,
                                     /*dst=*/out_offsets + elements_length));
    elements_length += offset_buffers[i]->size() / sizeof(offset_type);
    num_child_values += value_ranges[i].length;
    if (num_child_values > std::numeric_limits<offset_type>::max()) {
      return Status::Invalid("offset overflow while concatenating arrays");
    }
  }
  DCHECK_EQ(elements_length,
            static_cast<int64_t>(out_size_in_bytes / sizeof(offset_type)));

  return Status::OK();
}

template <typename offset_type>
Status PutListViewOffsets(const ArrayData& input, offset_type* sizes, const Buffer& src,
                          offset_type displacement, offset_type* dst) {
  if (src.size() == 0) {
    return Status::OK();
  }
  const auto& validity_buffer = input.buffers[0];
  if (validity_buffer) {
    // Ensure that it is safe to access all the bits in the validity bitmap of input.
    RETURN_NOT_OK(internal::CheckSliceParams(/*size=*/8 * validity_buffer->size(),
                                             input.offset, input.length, "buffer"));
  }

  const auto offsets = src.data_as<offset_type>();
  DCHECK_EQ(static_cast<int64_t>(src.size() / sizeof(offset_type)), input.length);

  auto visit_not_null = [&](int64_t position) {
    if (sizes[position] > 0) {
      // NOTE: Concatenate can be called during IPC reads to append delta
      // dictionaries. Avoid UB on non-validated input by doing the addition in the
      // unsigned domain. (the result can later be validated using
      // Array::ValidateFull)
      const auto displaced_offset = SafeSignedAdd(offsets[position], displacement);
      // displaced_offset>=0 is guaranteed by RangeOfValuesUsed returning the
      // smallest offset of valid and non-empty list-views.
      DCHECK_GE(displaced_offset, 0);
      dst[position] = displaced_offset;
    } else {
      // Do nothing to leave the dst[position] as 0.
    }
  };

  const auto* validity = validity_buffer ? validity_buffer->data_as<uint8_t>() : nullptr;
  internal::OptionalBitBlockCounter bit_counter(validity, input.offset, input.length);
  int64_t position = 0;
  while (position < input.length) {
    internal::BitBlockCount block = bit_counter.NextBlock();
    if (block.AllSet()) {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        visit_not_null(position);
      }
    } else if (block.NoneSet()) {
      // NOTE: we don't have to do anything for the null entries regarding the
      // offsets as the buffer is initialized to 0 when it is allocated.

      // Zero-out the sizes of the null entries to ensure these sizes are not
      // greater than the new values length of the concatenated array.
      memset(sizes + position, 0, block.length * sizeof(offset_type));
      position += block.length;
    } else {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        if (bit_util::GetBit(validity, input.offset + position)) {
          visit_not_null(position);
        } else {
          // Zero-out the size at position.
          sizes[position] = 0;
        }
      }
    }
  }
  return Status::OK();
}

class ConcatenateImpl {
 public:
  ConcatenateImpl(const ArrayDataVector& in, MemoryPool* pool)
      : in_(in), pool_(pool), out_(std::make_shared<ArrayData>()) {
    out_->type = in_[0]->type;
    for (const auto& in_array : in_) {
      out_->length = SafeSignedAdd(out_->length, in_array->length);
      if (out_->null_count == kUnknownNullCount ||
          in_array->null_count == kUnknownNullCount) {
        out_->null_count = kUnknownNullCount;
        continue;
      }
      out_->null_count =
          SafeSignedAdd(out_->null_count.load(), in_array->null_count.load());
    }
    out_->buffers.resize(in_[0]->buffers.size());
    out_->child_data.resize(in_[0]->child_data.size());
    for (auto& data : out_->child_data) {
      data = std::make_shared<ArrayData>();
    }
  }

  Status Concatenate(std::shared_ptr<ArrayData>* out) && {
    if (out_->null_count != 0 && internal::may_have_validity_bitmap(out_->type->id())) {
      RETURN_NOT_OK(ConcatenateBitmaps(Bitmaps(0), pool_, &out_->buffers[0]));
    }
    RETURN_NOT_OK(VisitTypeInline(*out_->type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    return ConcatenateBitmaps(Bitmaps(1), pool_, &out_->buffers[1]);
  }

  Status Visit(const FixedWidthType& fixed) {
    // Handles numbers, decimal128, decimal256, fixed_size_binary
    ARROW_ASSIGN_OR_RAISE(auto buffers, Buffers(1, fixed));
    return ConcatenateBuffers(buffers, pool_).Value(&out_->buffers[1]);
  }

  Status Visit(const BinaryType&) {
    std::vector<Range> value_ranges;
    ARROW_ASSIGN_OR_RAISE(auto index_buffers, Buffers(1, sizeof(int32_t)));
    RETURN_NOT_OK(ConcatenateOffsets<int32_t>(index_buffers, pool_, &out_->buffers[1],
                                              &value_ranges));
    ARROW_ASSIGN_OR_RAISE(auto value_buffers, Buffers(2, value_ranges));
    return ConcatenateBuffers(value_buffers, pool_).Value(&out_->buffers[2]);
  }

  Status Visit(const LargeBinaryType&) {
    std::vector<Range> value_ranges;
    ARROW_ASSIGN_OR_RAISE(auto index_buffers, Buffers(1, sizeof(int64_t)));
    RETURN_NOT_OK(ConcatenateOffsets<int64_t>(index_buffers, pool_, &out_->buffers[1],
                                              &value_ranges));
    ARROW_ASSIGN_OR_RAISE(auto value_buffers, Buffers(2, value_ranges));
    return ConcatenateBuffers(value_buffers, pool_).Value(&out_->buffers[2]);
  }

  Status Visit(const BinaryViewType& type) {
    out_->buffers.resize(2);

    for (const auto& in_data : in_) {
      for (const auto& buf : util::span(in_data->buffers).subspan(2)) {
        out_->buffers.push_back(buf);
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto view_buffers, Buffers(1, BinaryViewType::kSize));
    ARROW_ASSIGN_OR_RAISE(auto view_buffer, ConcatenateBuffers(view_buffers, pool_));

    auto* views = view_buffer->mutable_data_as<BinaryViewType::c_type>();
    size_t preceding_buffer_count = 0;

    int64_t i = in_[0]->length;
    for (size_t in_index = 1; in_index < in_.size(); ++in_index) {
      preceding_buffer_count += in_[in_index - 1]->buffers.size() - 2;

      for (int64_t end_i = i + in_[in_index]->length; i < end_i; ++i) {
        if (views[i].is_inline()) continue;
        views[i].ref.buffer_index = SafeSignedAdd(
            views[i].ref.buffer_index, static_cast<int32_t>(preceding_buffer_count));
      }
    }

    if (out_->buffers[0] != nullptr) {
      i = in_[0]->length;
      VisitNullBitmapInline(
          out_->buffers[0]->data(), i, out_->length - i, out_->null_count, [&] { ++i; },
          [&] {
            views[i++] = {};  // overwrite views under null bits with an empty view
          });
    }

    out_->buffers[1] = std::move(view_buffer);
    return Status::OK();
  }

  Status Visit(const ListType&) {
    std::vector<Range> value_ranges;
    ARROW_ASSIGN_OR_RAISE(auto index_buffers, Buffers(1, sizeof(int32_t)));
    RETURN_NOT_OK(ConcatenateOffsets<int32_t>(index_buffers, pool_, &out_->buffers[1],
                                              &value_ranges));
    ARROW_ASSIGN_OR_RAISE(auto child_data, ChildData(0, value_ranges));
    return ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[0]);
  }

  Status Visit(const LargeListType&) {
    std::vector<Range> value_ranges;
    ARROW_ASSIGN_OR_RAISE(auto index_buffers, Buffers(1, sizeof(int64_t)));
    RETURN_NOT_OK(ConcatenateOffsets<int64_t>(index_buffers, pool_, &out_->buffers[1],
                                              &value_ranges));
    ARROW_ASSIGN_OR_RAISE(auto child_data, ChildData(0, value_ranges));
    return ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[0]);
  }

  template <typename T>
  enable_if_list_view<T, Status> Visit(const T& type) {
    using offset_type = typename T::offset_type;
    out_->buffers.resize(3);
    out_->child_data.resize(1);

    // Calculate the ranges of values that each list-view array uses
    std::vector<Range> value_ranges;
    value_ranges.reserve(in_.size());
    for (const auto& input : in_) {
      ArraySpan input_span(*input);
      Range range;
      ARROW_ASSIGN_OR_RAISE(std::tie(range.offset, range.length),
                            list_util::internal::RangeOfValuesUsed(input_span));
      value_ranges.push_back(range);
    }

    // Concatenate the values
    ARROW_ASSIGN_OR_RAISE(ArrayDataVector value_data, ChildData(0, value_ranges));
    RETURN_NOT_OK(ConcatenateImpl(value_data, pool_).Concatenate(&out_->child_data[0]));
    out_->child_data[0]->type = type.value_type();

    // Concatenate the sizes first
    ARROW_ASSIGN_OR_RAISE(auto size_buffers, Buffers(2, sizeof(offset_type)));
    RETURN_NOT_OK(ConcatenateBuffers(size_buffers, pool_).Value(&out_->buffers[2]));

    // Concatenate the offsets
    ARROW_ASSIGN_OR_RAISE(auto offset_buffers, Buffers(1, sizeof(offset_type)));
    RETURN_NOT_OK(ConcatenateListViewOffsets<offset_type>(
        in_, /*sizes=*/out_->buffers[2]->mutable_data_as<offset_type>(), offset_buffers,
        value_ranges, pool_, &out_->buffers[1]));

    return Status::OK();
  }

  Status Visit(const FixedSizeListType& fixed_size_list) {
    ARROW_ASSIGN_OR_RAISE(auto child_data, ChildData(0, fixed_size_list.list_size()));
    return ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[0]);
  }

  Status Visit(const StructType& s) {
    for (int i = 0; i < s.num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto child_data, ChildData(i));
      RETURN_NOT_OK(ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[i]));
    }
    return Status::OK();
  }

  Result<BufferVector> UnifyDictionaries(const DictionaryType& d) {
    BufferVector new_index_lookup;
    ARROW_ASSIGN_OR_RAISE(auto unifier, DictionaryUnifier::Make(d.value_type()));
    new_index_lookup.resize(in_.size());
    for (size_t i = 0; i < in_.size(); i++) {
      auto item = in_[i];
      auto dictionary_array = MakeArray(item->dictionary);
      RETURN_NOT_OK(unifier->Unify(*dictionary_array, &new_index_lookup[i]));
    }
    std::shared_ptr<Array> out_dictionary;
    RETURN_NOT_OK(unifier->GetResultWithIndexType(d.index_type(), &out_dictionary));
    out_->dictionary = out_dictionary->data();
    return new_index_lookup;
  }

  // Transpose and concatenate dictionary indices
  Result<std::shared_ptr<Buffer>> ConcatenateDictionaryIndices(
      const DataType& index_type, const BufferVector& index_transpositions) {
    const auto index_width =
        internal::checked_cast<const FixedWidthType&>(index_type).bit_width() / 8;
    int64_t out_length = 0;
    for (const auto& data : in_) {
      out_length += data->length;
    }
    ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(out_length * index_width, pool_));
    uint8_t* out_data = out->mutable_data();
    for (size_t i = 0; i < in_.size(); i++) {
      const auto& data = in_[i];
      auto transpose_map =
          reinterpret_cast<const int32_t*>(index_transpositions[i]->data());
      const uint8_t* src = data->GetValues<uint8_t>(1, 0);
      if (!data->buffers[0]) {
        RETURN_NOT_OK(internal::TransposeInts(index_type, index_type,
                                              /*src=*/data->GetValues<uint8_t>(1, 0),
                                              /*dest=*/out_data,
                                              /*src_offset=*/data->offset,
                                              /*dest_offset=*/0, /*length=*/data->length,
                                              transpose_map));
      } else {
        internal::BitRunReader reader(data->buffers[0]->data(), data->offset,
                                      data->length);
        int64_t position = 0;
        while (true) {
          internal::BitRun run = reader.NextRun();
          if (run.length == 0) break;

          if (run.set) {
            RETURN_NOT_OK(internal::TransposeInts(index_type, index_type, src,
                                                  /*dest=*/out_data,
                                                  /*src_offset=*/data->offset + position,
                                                  /*dest_offset=*/position, run.length,
                                                  transpose_map));
          } else {
            std::fill(out_data + (position * index_width),
                      out_data + (position + run.length) * index_width, 0x00);
          }

          position += run.length;
        }
      }
      out_data += data->length * index_width;
    }
    // R build with openSUSE155 requires an explicit shared_ptr construction
    return std::shared_ptr<Buffer>(std::move(out));
  }

  Status Visit(const DictionaryType& d) {
    auto fixed = internal::checked_cast<const FixedWidthType*>(d.index_type().get());

    // Two cases: all the dictionaries are the same, or unification is
    // required
    bool dictionaries_same = true;
    std::shared_ptr<Array> dictionary0 = MakeArray(in_[0]->dictionary);
    for (size_t i = 1; i < in_.size(); ++i) {
      if (!MakeArray(in_[i]->dictionary)->Equals(dictionary0)) {
        dictionaries_same = false;
        break;
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto index_buffers, Buffers(1, *fixed));
    if (dictionaries_same) {
      out_->dictionary = in_[0]->dictionary;
      return ConcatenateBuffers(index_buffers, pool_).Value(&out_->buffers[1]);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto index_lookup, UnifyDictionaries(d));
      ARROW_ASSIGN_OR_RAISE(out_->buffers[1],
                            ConcatenateDictionaryIndices(*fixed, index_lookup));
      return Status::OK();
    }
  }

  Status Visit(const UnionType& u) {
    // This implementation assumes that all input arrays are valid union arrays
    // with same number of variants.

    // Concatenate the type buffers.
    ARROW_ASSIGN_OR_RAISE(auto type_buffers, Buffers(1, sizeof(int8_t)));
    RETURN_NOT_OK(ConcatenateBuffers(type_buffers, pool_).Value(&out_->buffers[1]));

    // Concatenate the child data. For sparse unions the child data is sliced
    // based on the offset and length of the array data. For dense unions the
    // child data is not sliced because this makes constructing the concatenated
    // offsets buffer more simple. We could however choose to modify this and
    // slice the child arrays and reflect this in the concatenated offsets
    // buffer.
    switch (u.mode()) {
      case UnionMode::SPARSE: {
        for (int i = 0; i < u.num_fields(); i++) {
          ARROW_ASSIGN_OR_RAISE(auto child_data, ChildData(i));
          RETURN_NOT_OK(
              ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[i]));
        }
        break;
      }
      case UnionMode::DENSE: {
        for (int i = 0; i < u.num_fields(); i++) {
          ArrayDataVector child_data(in_.size());
          for (size_t j = 0; j < in_.size(); j++) {
            child_data[j] = in_[j]->child_data[i];
          }
          RETURN_NOT_OK(
              ConcatenateImpl(child_data, pool_).Concatenate(&out_->child_data[i]));
        }
        break;
      }
    }

    // Concatenate offsets buffers for dense union arrays.
    if (u.mode() == UnionMode::DENSE) {
      // The number of offset values is equal to the number of type_ids in the
      // concatenated type buffers.
      TypedBufferBuilder<int32_t> builder;
      RETURN_NOT_OK(builder.Reserve(out_->length));

      // Initialize a vector for child array lengths. These are updated during
      // iteration over the input arrays to track the concatenated child array
      // lengths. These lengths are used as offsets for the concatenated offsets
      // buffer.
      std::vector<int32_t> offset_map(u.num_fields());

      // Iterate over all input arrays.
      for (size_t i = 0; i < in_.size(); i++) {
        // Get sliced type ids and offsets.
        auto type_ids = in_[i]->GetValues<int8_t>(1);
        auto offset_values = in_[i]->GetValues<int32_t>(2);

        // Iterate over all elements in the type buffer and append the updated
        // offset to the concatenated offsets buffer.
        for (auto j = 0; j < in_[i]->length; j++) {
          int32_t offset;
          if (internal::AddWithOverflow(offset_map[u.child_ids()[type_ids[j]]],
                                        offset_values[j], &offset)) {
            return Status::Invalid("Offset value overflow when concatenating arrays");
          }
          RETURN_NOT_OK(builder.Append(offset));
        }

        // Increment the offsets in the offset map for the next iteration.
        for (int j = 0; j < u.num_fields(); j++) {
          int64_t length;
          if (internal::AddWithOverflow(static_cast<int64_t>(offset_map[j]),
                                        in_[i]->child_data[j]->length, &length)) {
            return Status::Invalid("Offset value overflow when concatenating arrays");
          }
          // Make sure we can safely downcast to int32_t.
          if (length > std::numeric_limits<int32_t>::max()) {
            return Status::Invalid("Length overflow when concatenating arrays");
          }
          offset_map[j] = static_cast<int32_t>(length);
        }
      }

      ARROW_ASSIGN_OR_RAISE(out_->buffers[2], builder.Finish());
    }

    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    int64_t physical_length = 0;
    for (const auto& input : in_) {
      if (internal::AddWithOverflow(physical_length,
                                    ree_util::FindPhysicalLength(ArraySpan(*input)),
                                    &physical_length)) {
        return Status::Invalid("Length overflow when concatenating arrays");
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(in_[0]->type, pool_));
    RETURN_NOT_OK(internal::checked_cast<RunEndEncodedBuilder&>(*builder).ReservePhysical(
        physical_length));
    for (const auto& input : in_) {
      RETURN_NOT_OK(builder->AppendArraySlice(ArraySpan(*input), 0, input->length));
    }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> out_array, builder->Finish());
    out_ = out_array->data();
    return Status::OK();
  }

  Status Visit(const ExtensionType& e) {
    ArrayDataVector storage_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      storage_data[i] = in_[i]->Copy();
      storage_data[i]->type = e.storage_type();
    }
    std::shared_ptr<ArrayData> out_storage;
    RETURN_NOT_OK(ConcatenateImpl(storage_data, pool_).Concatenate(&out_storage));
    out_storage->type = in_[0]->type;
    out_ = std::move(out_storage);
    return Status::OK();
  }

 private:
  // NOTE: Concatenate() can be called during IPC reads to append delta dictionaries
  // on non-validated input.  Therefore, the input-checking SliceBufferSafe and
  // ArrayData::SliceSafe are used below.

  // Gather the index-th buffer of each input into a vector.
  // Bytes are sliced with that input's offset and length.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  Result<BufferVector> Buffers(size_t index) {
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (const auto& array_data : in_) {
      const auto& buffer = array_data->buffers[index];
      if (buffer != nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            auto sliced_buffer,
            SliceBufferSafe(buffer, array_data->offset, array_data->length));
        buffers.push_back(std::move(sliced_buffer));
      }
    }
    return buffers;
  }

  // Gather the index-th buffer of each input into a vector.
  // Bytes are sliced with the explicitly passed ranges.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  Result<BufferVector> Buffers(size_t index, const std::vector<Range>& ranges) {
    DCHECK_EQ(in_.size(), ranges.size());
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      const auto& buffer = in_[i]->buffers[index];
      if (buffer != nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            auto sliced_buffer,
            SliceBufferSafe(buffer, ranges[i].offset, ranges[i].length));
        buffers.push_back(std::move(sliced_buffer));
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
  Result<BufferVector> Buffers(size_t index, int byte_width) {
    BufferVector buffers;
    buffers.reserve(in_.size());
    for (const auto& array_data : in_) {
      const auto& buffer = array_data->buffers[index];
      if (buffer != nullptr) {
        ARROW_ASSIGN_OR_RAISE(auto sliced_buffer,
                              SliceBufferSafe(buffer, array_data->offset * byte_width,
                                              array_data->length * byte_width));
        buffers.push_back(std::move(sliced_buffer));
      }
    }
    return buffers;
  }

  // Gather the index-th buffer of each input into a vector.
  // Buffers are assumed to contain elements of fixed.bit_width(),
  // those elements are sliced with that input's offset and length.
  // Note that BufferVector will not contain the buffer of in_[i] if it's
  // nullptr.
  Result<BufferVector> Buffers(size_t index, const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    return Buffers(index, fixed.bit_width() / 8);
  }

  // Gather the index-th buffer of each input as a Bitmap
  // into a vector of Bitmaps.
  std::vector<Bitmap> Bitmaps(size_t index) {
    std::vector<Bitmap> bitmaps(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      Range range(in_[i]->offset, in_[i]->length);
      bitmaps[i] = Bitmap(in_[i]->buffers[index], range);
    }
    return bitmaps;
  }

  // Gather the index-th child_data of each input into a vector.
  // Elements are sliced with that input's offset and length.
  Result<ArrayDataVector> ChildData(size_t index) {
    ArrayDataVector child_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(child_data[i], in_[i]->child_data[index]->SliceSafe(
                                               in_[i]->offset, in_[i]->length));
    }
    return child_data;
  }

  // Gather the index-th child_data of each input into a vector.
  // Elements are sliced with that input's offset and length multiplied by multiplier.
  Result<ArrayDataVector> ChildData(size_t index, size_t multiplier) {
    ArrayDataVector child_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          child_data[i], in_[i]->child_data[index]->SliceSafe(
                             in_[i]->offset * multiplier, in_[i]->length * multiplier));
    }
    return child_data;
  }

  // Gather the index-th child_data of each input into a vector.
  // Elements are sliced with the explicitly passed ranges.
  Result<ArrayDataVector> ChildData(size_t index, const std::vector<Range>& ranges) {
    DCHECK_EQ(in_.size(), ranges.size());
    ArrayDataVector child_data(in_.size());
    for (size_t i = 0; i < in_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(child_data[i], in_[i]->child_data[index]->SliceSafe(
                                               ranges[i].offset, ranges[i].length));
    }
    return child_data;
  }

  const ArrayDataVector& in_;
  MemoryPool* pool_;
  std::shared_ptr<ArrayData> out_;
};

}  // namespace

Result<std::shared_ptr<Array>> Concatenate(const ArrayVector& arrays, MemoryPool* pool) {
  if (arrays.size() == 0) {
    return Status::Invalid("Must pass at least one array");
  }

  // gather ArrayData of input arrays
  ArrayDataVector data(arrays.size());
  for (size_t i = 0; i < arrays.size(); ++i) {
    if (!arrays[i]->type()->Equals(*arrays[0]->type())) {
      return Status::Invalid("arrays to be concatenated must be identically typed, but ",
                             *arrays[0]->type(), " and ", *arrays[i]->type(),
                             " were encountered.");
    }
    data[i] = arrays[i]->data();
  }

  std::shared_ptr<ArrayData> out_data;
  RETURN_NOT_OK(ConcatenateImpl(data, pool).Concatenate(&out_data));
  return MakeArray(std::move(out_data));
}

}  // namespace arrow
