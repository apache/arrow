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
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
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

// Write offsets in src into dst, adjusting them such that first_offset
// will be the first offset written.
template <typename Offset>
Status PutOffsets(const std::shared_ptr<Buffer>& src, Offset first_offset, Offset* dst,
                  Range* values_range);

// Concatenate buffers holding offsets into a single buffer of offsets,
// also computing the ranges of values spanned by each buffer of offsets.
template <typename Offset>
Status ConcatenateOffsets(const BufferVector& buffers, MemoryPool* pool,
                          std::shared_ptr<Buffer>* out,
                          std::vector<Range>* values_ranges) {
  values_ranges->resize(buffers.size());

  // allocate output buffer
  int64_t out_length = 0;
  for (const auto& buffer : buffers) {
    out_length += buffer->size() / sizeof(Offset);
  }
  ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer((out_length + 1) * sizeof(Offset), pool));
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
Status PutOffsets(const std::shared_ptr<Buffer>& src, Offset first_offset, Offset* dst,
                  Range* values_range) {
  if (src->size() == 0) {
    // It's allowed to have an empty offsets buffer for a 0-length array
    // (see Array::Validate)
    values_range->offset = 0;
    values_range->length = 0;
    return Status::OK();
  }

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
  // NOTE: Concatenate can be called during IPC reads to append delta dictionaries.
  // Avoid UB on non-validated input by doing the addition in the unsigned domain.
  // (the result can later be validated using Array::ValidateFull)
  std::transform(src_begin, src_end, dst, [adjustment](Offset offset) {
    return SafeSignedAdd(offset, adjustment);
  });
  return Status::OK();
}

class ConcatenateImpl {
 public:
  ConcatenateImpl(const ArrayDataVector& in, MemoryPool* pool)
      : in_(std::move(in)), pool_(pool), out_(std::make_shared<ArrayData>()) {
    out_->type = in[0]->type;
    for (size_t i = 0; i < in_.size(); ++i) {
      out_->length = SafeSignedAdd(out_->length, in[i]->length);
      if (out_->null_count == kUnknownNullCount ||
          in[i]->null_count == kUnknownNullCount) {
        out_->null_count = kUnknownNullCount;
        continue;
      }
      out_->null_count = SafeSignedAdd(out_->null_count.load(), in[i]->null_count.load());
    }
    out_->buffers.resize(in[0]->buffers.size());
    out_->child_data.resize(in[0]->child_data.size());
    for (auto& data : out_->child_data) {
      data = std::make_shared<ArrayData>();
    }
  }

  Status Concatenate(std::shared_ptr<ArrayData>* out) && {
    if (out_->null_count != 0 && internal::HasValidityBitmap(out_->type->id())) {
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
    return std::move(out);
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

  Status Visit(const RunLengthEncodedType& type) {
    return Status::NotImplemented("concatenation of ", type);
  }

  Status Visit(const ExtensionType& e) {
    // XXX can we just concatenate their storage?
    return Status::NotImplemented("concatenation of ", e);
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
