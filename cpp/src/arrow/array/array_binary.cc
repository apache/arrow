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

#include "arrow/array/array_binary.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <set>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/util.h"
#include "arrow/array/validate.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

BinaryArray::BinaryArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_binary_like(data->type->id()));
  SetData(data);
}

BinaryArray::BinaryArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(binary(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

LargeBinaryArray::LargeBinaryArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_large_binary_like(data->type->id()));
  SetData(data);
}

LargeBinaryArray::LargeBinaryArray(int64_t length,
                                   const std::shared_ptr<Buffer>& value_offsets,
                                   const std::shared_ptr<Buffer>& data,
                                   const std::shared_ptr<Buffer>& null_bitmap,
                                   int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(large_binary(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

StringArray::StringArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING);
  SetData(data);
}

StringArray::StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(utf8(), length, {null_bitmap, value_offsets, data}, null_count,
                          offset));
}

Status StringArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

LargeStringArray::LargeStringArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::LARGE_STRING);
  SetData(data);
}

LargeStringArray::LargeStringArray(int64_t length,
                                   const std::shared_ptr<Buffer>& value_offsets,
                                   const std::shared_ptr<Buffer>& data,
                                   const std::shared_ptr<Buffer>& null_bitmap,
                                   int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(large_utf8(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

Status LargeStringArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

BinaryViewArray::BinaryViewArray(std::shared_ptr<ArrayData> data) {
  ARROW_CHECK_EQ(data->type->id(), Type::BINARY_VIEW);
  SetData(std::move(data));
}

BinaryViewArray::BinaryViewArray(std::shared_ptr<DataType> type, int64_t length,
                                 std::shared_ptr<Buffer> views, BufferVector buffers,
                                 std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                                 int64_t offset) {
  buffers.insert(buffers.begin(), std::move(views));
  buffers.insert(buffers.begin(), std::move(null_bitmap));
  SetData(
      ArrayData::Make(std::move(type), length, std::move(buffers), null_count, offset));
}

namespace {

// TODO Should We move this to bitmap_ops.h and Remove from compute/kernels/util.s
Result<std::shared_ptr<Buffer>> GetOrCopyNullBitmapBuffer(const ArrayData& in_array,
                                                          MemoryPool* pool) {
  if (in_array.buffers[0]->data() == nullptr) {
    return nullptr;
  } else if (in_array.offset == 0) {
    return in_array.buffers[0];
  } else if (in_array.offset % 8 == 0) {
    return SliceBuffer(in_array.buffers[0], /*offset=*/in_array.offset / 8);
  } else {
    // If a non-zero offset, we need to shift the bitmap
    return internal::CopyBitmap(pool, in_array.buffers[0]->data(), in_array.offset,
                                in_array.length);
  }
}

struct Interval {
  int64_t start;
  int64_t end;
  int32_t offset = -1;
};

struct IntervalComparator {
  bool operator()(const Interval& left, const Interval& right) const {
    return left.start < right.start;
  }
};

// inspired from boost::icl::interval_set
class IntervalMerger {
 public:
  using IntervalSet = std::set<Interval, IntervalComparator>;
  using Iterator = std::set<Interval, IntervalComparator>::iterator;

  void AddInterval(const Interval& interval) {
    auto [it, is_inserted] = interval_set.insert(interval);
    if (is_inserted) {
      JointLeft(it);
      JoinRight(it);
    } else {
      if (it->end < interval.end) {
        const_cast<int64_t&>(it->end) = interval.end;
        JoinRight(it);
      }
    }
  }

  int64_t CalculateOffsetAndTotalSize() {
    int64_t total_size = 0;
    for (auto& it : interval_set) {
      const_cast<int32_t&>(it.offset) = static_cast<int32_t>(total_size);
      total_size += it.end - it.start;
    }
    return total_size;
  }

  // This method should be called After CalculateOffsetAndTotalSize
  int32_t GetRelativeOffset(int32_t view_offset) const {
    auto it = interval_set.lower_bound({view_offset});
    if (it == interval_set.end()) {
      --it;
      // offset from the start of interval
      auto offset_from_span = view_offset - it->start;
      return static_cast<int32_t>(offset_from_span) + it->offset;
    } else if (it->start == view_offset) {
      // this is the case where view_offset refers to the beginning of interval
      return it->offset;
    } else {
      --it;
      // offset from the start of interval
      auto offset_from_span = view_offset - it->start;
      return static_cast<int32_t>(offset_from_span) + it->offset;
    }
  }

  IntervalSet::const_iterator begin() const { return interval_set.cbegin(); }

  IntervalSet::const_iterator end() const { return interval_set.cend(); }

 private:
  void JointLeft(Iterator& it) {
    if (it == interval_set.begin()) {
      return;
    } else {
      auto prev_it = std::prev(it);
      if (Joinable(prev_it, it)) {
        MergeIntoLeftAndAdvanceRight(prev_it, it);
        it = prev_it;
        return;
      }
    }
  }

  void JoinRight(Iterator& it) {
    auto begin_iterator = std::next(it);
    auto end_iterator = begin_iterator;
    while (end_iterator != interval_set.end() && Joinable(it, end_iterator)) {
      const_cast<int64_t&>(it->end) = std::max(it->end, end_iterator->end);
      ++end_iterator;
    }
    interval_set.erase(begin_iterator, end_iterator);
  }

  // Update left with a new end value
  // Advance right to the next iterator
  void MergeIntoLeftAndAdvanceRight(Iterator& left, Iterator& right) {
    Interval interval{right->start, right->end};
    right = interval_set.erase(right);
    const_cast<int64_t&>(left->end) = std::max(left->end, interval.end);
  }

  bool Joinable(Iterator left, Iterator right) {
    return std::max(left->start, right->start) <= std::min(left->end, right->end);
  }

  IntervalSet interval_set;
};

class CompactArrayImpl {
 public:
  CompactArrayImpl(const std::shared_ptr<ArrayData>& src_array_data,
                   double occupancy_threshold, MemoryPool* memory_pool)
      : src_array_data_(src_array_data),
        src_buffers_(src_array_data->buffers),
        occupancy_threshold_(occupancy_threshold),
        memory_pool_(memory_pool) {}

  Result<std::shared_ptr<Array>> Compact() {
    // Check occupancy_threshold Parameter Validity
    if (ARROW_PREDICT_FALSE(ValidateOccupancyThreshold(occupancy_threshold_))) {
      return Status::Invalid(
          "occupancy_threshold must be between 0 and 1. Current value:",
          occupancy_threshold_);
    }

    auto num_src_buffers = src_buffers_.size();

    if (ARROW_PREDICT_FALSE(num_src_buffers < 2)) {
      return Status::Invalid("The number of buffers in ArrayData is less than 2.");
    } else if (num_src_buffers == 2) {
      // Only the bitmap and view buffers are available.
      // Should we copy the view buffer to reduce size if an offset is set?
      dst_buffers_.insert(dst_buffers_.end(), src_buffers_.begin(), src_buffers_.end());
      return MakeArray(ArrayData::Make(
          src_array_data_->type, src_array_data_->length, std::move(dst_buffers_),
          src_array_data_->null_count, src_array_data_->offset));
    } else {
      ARROW_RETURN_NOT_OK(AddBitmapBuffer());

      ARROW_RETURN_NOT_OK(AddViewBuffer());

      // Handle DataBuffer
      auto buffer_infos = GenerateBufferInfos();

      // Relocating buffers whose occupancy is non-zero and below the threshold.
      ARROW_RETURN_NOT_OK(CompactDataBufferBasedBufferInfo(buffer_infos));

      AdjustViewElementsBufferIndexAndOffset(buffer_infos);

      return MakeArray(ArrayData::Make(src_array_data_->type, src_array_data_->length,
                                       std::move(dst_buffers_)));
    }
  }

 private:
  struct BufferInfo {
    // It is possible that total occupancy of a data buffer
    // becomes higher than MAX_INT32_T.
    int64_t total_size_occupied = 0;
    int32_t new_index = -1;
    // offset in new buffer
    // it is used when buffer is compacted
    int32_t base_new_offset = -1;
    IntervalMerger interval_merger;
    // True if occupancy is non-zero and
    // less than or equal to the threshold.
    bool should_be_relocated = false;
  };

  struct BufferIndexAndOffsetMapper {
    // The buffer will be copied.
    // Returns the index and offset in the new buffer.
    std::pair<int32_t, int32_t> MergeBufferAndGetPosition(int64_t size) {
      int32_t buffer_offset;
      if (current_index == -1) {
        buffer_offset = 0;
        buffer_sizes.push_back(size);
        indexes.push_back(-1);
        current_index = static_cast<int32_t>(buffer_sizes.size()) - 1;
      } else if (buffer_sizes[current_index] + size >
                 std::numeric_limits<int32_t>::max()) {
        buffer_sizes.push_back(size);
        indexes.push_back(-1);
        current_index = static_cast<int32_t>(buffer_sizes.size()) - 1;
        buffer_offset = 0;
      } else {
        buffer_offset = static_cast<int32_t>(buffer_sizes[current_index]);
        buffer_sizes[current_index] += size;
      }
      return std::make_pair(current_index, buffer_offset);
    }

    // The buffer will not be copied.
    int32_t AppendBufferAndGetPosition(int64_t size, int32_t index) {
      buffer_sizes.push_back(size);
      indexes.push_back(index);
      return static_cast<int32_t>(buffer_sizes.size()) - 1;
    }

    std::vector<int64_t> buffer_sizes{};

    // Index from previous if it's not merged
    // The value whether is -1 for  a merged buffer or
    //  non-negative for  a non-merged-buffer.
    std::vector<int32_t> indexes{};
    // Uses for merging buffer to proper location
    int32_t current_index = -1;
  };

  bool ValidateOccupancyThreshold(double occupancy_threshold) {
    return std::signbit(occupancy_threshold) || std::isnan(occupancy_threshold) ||
           occupancy_threshold > 1;
  }

  Status AddBitmapBuffer() {
    if (src_array_data_->buffers[0] == nullptr) {
      dst_buffers_.emplace_back(nullptr);
    } else {
      // Handle Bitmap Buffer
      ARROW_ASSIGN_OR_RAISE(auto bitmap_buffer,
                            GetOrCopyNullBitmapBuffer(*src_array_data_, memory_pool_));
      dst_buffers_.push_back(bitmap_buffer);
    }
    return Status::OK();
  }

  Status AddViewBuffer() {
    ARROW_ASSIGN_OR_RAISE(
        auto view_buffer,
        src_array_data_->buffers[1]->CopySlice(
            src_array_data_->offset * BinaryViewType::kSize,
            src_array_data_->length * BinaryViewType::kSize, memory_pool_));
    dst_buffers_.push_back(view_buffer);
    return Status::OK();
  }

  std::vector<BufferInfo> GenerateBufferInfos() {
    using ViewType = BinaryViewType::c_type;

    auto view_buffer = src_array_data_->GetValues<ViewType>(1);

    // Ignore BitMap Buffer and View Buffer
    std::vector<BufferInfo> buffer_info_array(src_buffers_.size() - 2);

    auto visit = [&](int64_t position, int64_t length) {
      for (int64_t i = position; i < position + length; ++i) {
        auto& view = view_buffer[i];
        if (!view.is_inline()) {
          AddIntervalToBufferInfo(buffer_info_array, view);
        }
      }
    };

    internal::VisitSetBitRunsVoid(src_buffers_[0], src_array_data_->offset,
                                  src_array_data_->length, visit);
    return buffer_info_array;
  }

  void AddIntervalToBufferInfo(std::vector<BufferInfo>& buffer_infos,
                               const BinaryViewType::c_type& c_type) {
    auto& buffer_info = buffer_infos[c_type.ref.buffer_index];

    buffer_info.interval_merger.AddInterval(
        {c_type.ref.offset, static_cast<int64_t>(c_type.ref.offset) + c_type.ref.size});
  }

  // Relocating Buffers which their occupancies are less than threshold
  Status CompactDataBufferBasedBufferInfo(std::vector<BufferInfo>& buffer_infos) {
    auto num_src_data_buffers = static_cast<int32_t>(src_buffers_.size()) - 2;
    BufferIndexAndOffsetMapper buffer_mapper;

    for (int32_t i = 0; i < num_src_data_buffers; ++i) {
      CalculateOccupancyAndOffset(buffer_infos[i]);
      if (buffer_infos[i].total_size_occupied == 0) {
        // Ignore adding to new buffer
        buffer_infos[i].should_be_relocated = false;
      } else if (static_cast<double>(buffer_infos[i].total_size_occupied) /
                     static_cast<double>(src_buffers_[i + 2]->size()) <=
                 occupancy_threshold_) {
        // Calculate the size and offset in new Data Buffer
        auto [index, offset] =
            buffer_mapper.MergeBufferAndGetPosition(buffer_infos[i].total_size_occupied);
        buffer_infos[i].new_index = index;
        buffer_infos[i].base_new_offset = offset;
        buffer_infos[i].should_be_relocated = true;
      } else {
        buffer_infos[i].new_index = buffer_mapper.AppendBufferAndGetPosition(
            buffer_infos[i].total_size_occupied, i);
        buffer_infos[i].should_be_relocated = false;
      }
    }

    ARROW_RETURN_NOT_OK(GenerateDataBufferForDestination(buffer_mapper));
    CopyDataBuffer(buffer_infos);
    return Status::OK();
  }

  void CalculateOccupancyAndOffset(BufferInfo& info) {
    info.total_size_occupied = info.interval_merger.CalculateOffsetAndTotalSize();
  }

  Status GenerateDataBufferForDestination(
      const BufferIndexAndOffsetMapper& buffer_mapper) {
    // First Allocated Or Added Buffer
    dst_buffers_.reserve(buffer_mapper.buffer_sizes.size());
    for (int32_t i = 0; i < static_cast<int32_t>(buffer_mapper.buffer_sizes.size());
         ++i) {
      if (buffer_mapper.indexes[i] == -1) {
        ARROW_ASSIGN_OR_RAISE(
            auto buffer, AllocateBuffer(buffer_mapper.buffer_sizes[i], memory_pool_));
        dst_buffers_.push_back(std::move(buffer));
      } else {
        dst_buffers_.push_back(src_buffers_[2 + buffer_mapper.indexes[i]]);
      }
    }
    return Status::OK();
  }

  void CopyDataBuffer(const std::vector<BufferInfo>& buffer_infos) {
    for (int32_t i = 0; i < static_cast<int32_t>(buffer_infos.size()); ++i) {
      auto& buffer_info = buffer_infos[i];
      if (buffer_info.should_be_relocated) {
        // +2 to ignore view and data buffer
        const auto& src_data_buffers = src_buffers_[i + 2];
        const auto& dst_data_buffer = dst_buffers_[buffer_info.new_index + 2];
        for (auto interval : buffer_info.interval_merger) {
          std::memcpy(dst_data_buffer->mutable_data() + buffer_info.base_new_offset +
                          interval.offset,
                      src_data_buffers->data() + interval.start,
                      interval.end - interval.start);
        }
      }
    }
  }

  void AdjustViewElementsBufferIndexAndOffset(
      const std::vector<BufferInfo>& buffer_infos) {
    auto view_buffer = dst_buffers_[1]->mutable_data_as<BinaryViewArray::c_type>();

    auto Visitor = [&](int64_t position, int64_t number_of_elements) {
      for (int64_t i = position; i < position + number_of_elements; ++i) {
        auto& view = view_buffer[i];
        if (!view.is_inline()) {
          auto& info = buffer_infos[view.ref.buffer_index];
          view.ref.buffer_index = info.new_index;

          // Buffer is less than threshold and relocated
          if (info.should_be_relocated) {
            view.ref.offset = info.interval_merger.GetRelativeOffset(view.ref.offset) +
                              info.base_new_offset;
          }
        }
      }
    };

    internal::VisitSetBitRunsVoid(dst_buffers_[0], 0, src_array_data_->length, Visitor);
  }

  const std::shared_ptr<ArrayData> src_array_data_;
  std::vector<std::shared_ptr<Buffer>>& src_buffers_;
  std::vector<std::shared_ptr<Buffer>> dst_buffers_ = {};
  double occupancy_threshold_;
  MemoryPool* memory_pool_;
};

}  // namespace

Result<std::shared_ptr<Array>> BinaryViewArray::CompactArray(double occupancy_threshold,
                                                             MemoryPool* pool) const {
  return CompactArrayImpl(this->data(), occupancy_threshold, pool).Compact();
}

std::string_view BinaryViewArray::GetView(int64_t i) const {
  const std::shared_ptr<Buffer>* data_buffers = data_->buffers.data() + 2;
  return util::FromBinaryView(raw_values_[i], data_buffers);
}

StringViewArray::StringViewArray(std::shared_ptr<ArrayData> data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING_VIEW);
  SetData(std::move(data));
}

Status StringViewArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, data}, null_count, offset));
}

}  // namespace arrow
