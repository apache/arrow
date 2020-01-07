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

#pragma once

#include <algorithm>
#include <array>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/compare.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/string_view.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

class BitmapReader {
 public:
  BitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_offset_ = start_offset % 8;
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    }
  }

  bool IsSet() const { return (current_byte_ & (1 << bit_offset_)) != 0; }

  bool IsNotSet() const { return (current_byte_ & (1 << bit_offset_)) == 0; }

  void Next() {
    ++bit_offset_;
    ++position_;
    if (ARROW_PREDICT_FALSE(bit_offset_ == 8)) {
      bit_offset_ = 0;
      ++byte_offset_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

 private:
  const uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  int64_t byte_offset_;
  int64_t bit_offset_;
};

class BitmapWriter {
  // A sequential bitwise writer that preserves surrounding bit values.

 public:
  BitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    byte_offset_ = start_offset / 8;
    bit_mask_ = BitUtil::kBitmask[start_offset % 8];
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    } else {
      current_byte_ = 0;
    }
  }

  void Set() { current_byte_ |= bit_mask_; }

  void Clear() { current_byte_ &= bit_mask_ ^ 0xFF; }

  void Next() {
    bit_mask_ = static_cast<uint8_t>(bit_mask_ << 1);
    ++position_;
    if (bit_mask_ == 0) {
      // Finished this byte, need advancing
      bit_mask_ = 0x01;
      bitmap_[byte_offset_++] = current_byte_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

  void Finish() {
    // Store current byte if we didn't went past bitmap storage
    if (length_ > 0 && (bit_mask_ != 0x01 || position_ < length_)) {
      bitmap_[byte_offset_] = current_byte_;
    }
  }

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  uint8_t bit_mask_;
  int64_t byte_offset_;
};

class FirstTimeBitmapWriter {
  // Like BitmapWriter, but any bit values *following* the bits written
  // might be clobbered.  It is hence faster than BitmapWriter, and can
  // also avoid false positives with Valgrind.

 public:
  FirstTimeBitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_mask_ = BitUtil::kBitmask[start_offset % 8];
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_] & BitUtil::kPrecedingBitmask[start_offset % 8];
    } else {
      current_byte_ = 0;
    }
  }

  void Set() { current_byte_ |= bit_mask_; }

  void Clear() {}

  void Next() {
    bit_mask_ = static_cast<uint8_t>(bit_mask_ << 1);
    ++position_;
    if (bit_mask_ == 0) {
      // Finished this byte, need advancing
      bit_mask_ = 0x01;
      bitmap_[byte_offset_++] = current_byte_;
      current_byte_ = 0;
    }
  }

  void Finish() {
    // Store current byte if we didn't went past bitmap storage
    if (length_ > 0 && (bit_mask_ != 0x01 || position_ < length_)) {
      bitmap_[byte_offset_] = current_byte_;
    }
  }

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  uint8_t bit_mask_;
  int64_t byte_offset_;
};

// A std::generate() like function to write sequential bits into a bitmap area.
// Bits preceding the bitmap area are preserved, bits following the bitmap
// area may be clobbered.

template <class Generator>
void GenerateBits(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
  if (length == 0) {
    return;
  }
  uint8_t* cur = bitmap + start_offset / 8;
  uint8_t bit_mask = BitUtil::kBitmask[start_offset % 8];
  uint8_t current_byte = *cur & BitUtil::kPrecedingBitmask[start_offset % 8];

  for (int64_t index = 0; index < length; ++index) {
    const bool bit = g();
    current_byte = bit ? (current_byte | bit_mask) : current_byte;
    bit_mask = static_cast<uint8_t>(bit_mask << 1);
    if (bit_mask == 0) {
      bit_mask = 1;
      *cur++ = current_byte;
      current_byte = 0;
    }
  }
  if (bit_mask != 1) {
    *cur++ = current_byte;
  }
}

// Like GenerateBits(), but unrolls its main loop for higher performance.

template <class Generator>
void GenerateBitsUnrolled(uint8_t* bitmap, int64_t start_offset, int64_t length,
                          Generator&& g) {
  if (length == 0) {
    return;
  }
  uint8_t current_byte;
  uint8_t* cur = bitmap + start_offset / 8;
  const uint64_t start_bit_offset = start_offset % 8;
  uint8_t bit_mask = BitUtil::kBitmask[start_bit_offset];
  int64_t remaining = length;

  if (bit_mask != 0x01) {
    current_byte = *cur & BitUtil::kPrecedingBitmask[start_bit_offset];
    while (bit_mask != 0 && remaining > 0) {
      current_byte = g() ? (current_byte | bit_mask) : current_byte;
      bit_mask = static_cast<uint8_t>(bit_mask << 1);
      --remaining;
    }
    *cur++ = current_byte;
  }

  int64_t remaining_bytes = remaining / 8;
  while (remaining_bytes-- > 0) {
    current_byte = 0;
    current_byte = g() ? current_byte | 0x01 : current_byte;
    current_byte = g() ? current_byte | 0x02 : current_byte;
    current_byte = g() ? current_byte | 0x04 : current_byte;
    current_byte = g() ? current_byte | 0x08 : current_byte;
    current_byte = g() ? current_byte | 0x10 : current_byte;
    current_byte = g() ? current_byte | 0x20 : current_byte;
    current_byte = g() ? current_byte | 0x40 : current_byte;
    current_byte = g() ? current_byte | 0x80 : current_byte;
    *cur++ = current_byte;
  }

  int64_t remaining_bits = remaining % 8;
  if (remaining_bits) {
    current_byte = 0;
    bit_mask = 0x01;
    while (remaining_bits-- > 0) {
      current_byte = g() ? (current_byte | bit_mask) : current_byte;
      bit_mask = static_cast<uint8_t>(bit_mask << 1);
    }
    *cur++ = current_byte;
  }
}

// A function that visits each bit in a bitmap and calls a visitor function with a
// boolean representation of that bit. This is intended to be analogous to
// GenerateBits.
template <class Visitor>
void VisitBits(const uint8_t* bitmap, int64_t start_offset, int64_t length,
               Visitor&& visit) {
  BitmapReader reader(bitmap, start_offset, length);
  for (int64_t index = 0; index < length; ++index) {
    visit(reader.IsSet());
    reader.Next();
  }
}

// Like VisitBits(), but unrolls its main loop for better performance.
template <class Visitor>
void VisitBitsUnrolled(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                       Visitor&& visit) {
  if (length == 0) {
    return;
  }

  // Start by visiting any bits preceding the first full byte.
  int64_t num_bits_before_full_bytes =
      BitUtil::RoundUpToMultipleOf8(start_offset) - start_offset;
  // Truncate num_bits_before_full_bytes if it is greater than length.
  if (num_bits_before_full_bytes > length) {
    num_bits_before_full_bytes = length;
  }
  // Use the non loop-unrolled VisitBits since we don't want to add branches
  VisitBits<Visitor>(bitmap, start_offset, num_bits_before_full_bytes, visit);

  // Shift the start pointer to the first full byte and compute the
  // number of full bytes to be read.
  const uint8_t* first_full_byte = bitmap + BitUtil::CeilDiv(start_offset, 8);
  const int64_t num_full_bytes = (length - num_bits_before_full_bytes) / 8;

  // Iterate over each full byte of the input bitmap and call the visitor in
  // a loop-unrolled manner.
  for (int64_t byte_index = 0; byte_index < num_full_bytes; ++byte_index) {
    // Get the current bit-packed byte value from the bitmap.
    const uint8_t byte = *(first_full_byte + byte_index);

    // Execute the visitor function on each bit of the current byte.
    visit(BitUtil::GetBitFromByte(byte, 0));
    visit(BitUtil::GetBitFromByte(byte, 1));
    visit(BitUtil::GetBitFromByte(byte, 2));
    visit(BitUtil::GetBitFromByte(byte, 3));
    visit(BitUtil::GetBitFromByte(byte, 4));
    visit(BitUtil::GetBitFromByte(byte, 5));
    visit(BitUtil::GetBitFromByte(byte, 6));
    visit(BitUtil::GetBitFromByte(byte, 7));
  }

  // Write any leftover bits in the last byte.
  const int64_t num_bits_after_full_bytes = (length - num_bits_before_full_bytes) % 8;
  VisitBits<Visitor>(first_full_byte + num_full_bytes, 0, num_bits_after_full_bytes,
                     visit);
}

// ----------------------------------------------------------------------
// Bitmap utilities

class ARROW_EXPORT Bitmap : public util::ToStringOstreamable<Bitmap>,
                            public util::EqualityComparable<Bitmap> {
 public:
  template <typename Word>
  using View = util::basic_string_view<Word>;

  Bitmap() = default;

  Bitmap(std::shared_ptr<Buffer> buffer, int64_t offset, int64_t length)
      : buffer_(std::move(buffer)), offset_(offset), length_(length) {}

  Bitmap(const void* data, int64_t offset, int64_t length)
      : buffer_(std::make_shared<Buffer>(static_cast<const uint8_t*>(data),
                                         BitUtil::BytesForBits(offset + length))),
        offset_(offset),
        length_(length) {}

  Bitmap(void* data, int64_t offset, int64_t length)
      : buffer_(std::make_shared<MutableBuffer>(static_cast<uint8_t*>(data),
                                                BitUtil::BytesForBits(offset + length))),
        offset_(offset),
        length_(length) {}

  Bitmap Slice(int64_t offset) const {
    return Bitmap(buffer_, offset_ + offset, length_ - offset);
  }

  Bitmap Slice(int64_t offset, int64_t length) const {
    return Bitmap(buffer_, offset_ + offset, length);
  }

  std::string ToString() const;

  bool Equals(const Bitmap& other) const;

  std::string Diff(const Bitmap& other) const;

  bool GetBit(int64_t i) const { return BitUtil::GetBit(buffer_->data(), i + offset_); }

  bool operator[](int64_t i) const { return GetBit(i); }

  void SetBitTo(int64_t i, bool v) const {
    BitUtil::SetBitTo(buffer_->mutable_data(), i + offset_, v);
  }

  /// \brief Visit bits from each bitmap as bitset<N>
  ///
  /// All bitmaps must have identical length.
  template <size_t N, typename Visitor>
  static void VisitBits(const Bitmap (&bitmaps)[N], Visitor&& visitor) {
    int64_t bit_length = BitLength(bitmaps, N);
    std::bitset<N> bits;
    for (int64_t bit_i = 0; bit_i < bit_length; ++bit_i) {
      for (size_t i = 0; i < N; ++i) {
        bits[i] = bitmaps[i].GetBit(bit_i);
      }
      visitor(bits);
    }
  }

  /// \brief Visit words of bits from each bitmap as array<Word, N>
  ///
  /// All bitmaps must have identical length. The first bit in a visited bitmap
  /// may be offset within the first visited word, but words will otherwise contain
  /// densely packed bits loaded from the bitmap. That offset within the first word is
  /// returned.
  ///
  /// TODO(bkietz) allow for early termination
  template <size_t N, typename Visitor,
            typename Word =
                typename internal::call_traits::argument_type<0, Visitor&&>::value_type>
  static int64_t VisitWords(const Bitmap (&bitmaps_arg)[N], Visitor&& visitor) {
    constexpr int64_t kBitWidth = sizeof(Word) * 8;

    // local, mutable variables which will be sliced/decremented to represent consumption:
    Bitmap bitmaps[N];
    int64_t offsets[N];
    int64_t bit_length = BitLength(bitmaps_arg, N);
    View<Word> words[N];
    for (size_t i = 0; i < N; ++i) {
      bitmaps[i] = bitmaps_arg[i];
      offsets[i] = bitmaps[i].template word_offset<Word>();
      assert(offsets[i] >= 0 && offsets[i] < kBitWidth);
      words[i] = bitmaps[i].template words<Word>();
    }

    auto consume = [&](int64_t consumed_bits) {
      for (size_t i = 0; i < N; ++i) {
        bitmaps[i] = bitmaps[i].Slice(consumed_bits, bit_length - consumed_bits);
        offsets[i] = bitmaps[i].template word_offset<Word>();
        assert(offsets[i] >= 0 && offsets[i] < kBitWidth);
        words[i] = bitmaps[i].template words<Word>();
      }
      bit_length -= consumed_bits;
    };

    std::array<Word, N> visited_words;
    visited_words.fill(0);

    if (bit_length <= kBitWidth * 2) {
      // bitmaps fit into one or two words so don't bother with optimization
      while (bit_length > 0) {
        auto leading_bits = std::min(bit_length, kBitWidth);
        SafeLoadWords(bitmaps, 0, leading_bits, false, &visited_words);
        visitor(visited_words);
        consume(leading_bits);
      }
      return 0;
    }

    int64_t max_offset = *std::max_element(offsets, offsets + N);
    int64_t min_offset = *std::min_element(offsets, offsets + N);
    if (max_offset > 0) {
      // consume leading bits
      auto leading_bits = kBitWidth - min_offset;
      SafeLoadWords(bitmaps, 0, leading_bits, true, &visited_words);
      visitor(visited_words);
      consume(leading_bits);
    }
    assert(*std::min_element(offsets, offsets + N) == 0);

    int64_t whole_word_count = bit_length / kBitWidth;
    assert(whole_word_count >= 1);

    if (min_offset == max_offset) {
      // all offsets were identical, all leading bits have been consumed
      assert(
          std::all_of(offsets, offsets + N, [](int64_t offset) { return offset == 0; }));

      for (int64_t word_i = 0; word_i < whole_word_count; ++word_i) {
        for (size_t i = 0; i < N; ++i) {
          visited_words[i] = words[i][word_i];
        }
        visitor(visited_words);
      }
      consume(whole_word_count * kBitWidth);
    } else {
      // leading bits from potentially incomplete words have been consumed

      // word_i such that words[i][word_i] and words[i][word_i + 1] are lie entirely
      // within the bitmap for all i
      for (int64_t word_i = 0; word_i < whole_word_count - 1; ++word_i) {
        for (size_t i = 0; i < N; ++i) {
          if (offsets[i] == 0) {
            visited_words[i] = words[i][word_i];
          } else {
            visited_words[i] = words[i][word_i] >> offsets[i];
            visited_words[i] |= words[i][word_i + 1] << (kBitWidth - offsets[i]);
          }
        }
        visitor(visited_words);
      }
      consume((whole_word_count - 1) * kBitWidth);

      SafeLoadWords(bitmaps, 0, kBitWidth, false, &visited_words);

      visitor(visited_words);
      consume(kBitWidth);
    }

    // load remaining bits
    if (bit_length > 0) {
      SafeLoadWords(bitmaps, 0, bit_length, false, &visited_words);
      visitor(visited_words);
    }

    return min_offset;
  }

  const std::shared_ptr<Buffer>& buffer() const { return buffer_; }

  /// offset of first bit relative to buffer().data()
  int64_t offset() const { return offset_; }

  /// number of bits in this Bitmap
  int64_t length() const { return length_; }

  /// string_view of all bytes which contain any bit in this Bitmap
  util::bytes_view bytes() const {
    auto byte_offset = offset_ / 8;
    auto byte_count = BitUtil::CeilDiv(offset_ + length_, 8) - byte_offset;
    return util::bytes_view(buffer_->data() + byte_offset, byte_count);
  }

 private:
  /// string_view of all Words which contain any bit in this Bitmap
  ///
  /// For example, given Word=uint16_t and a bitmap spanning bits [20, 36)
  /// words() would span bits [16, 48).
  ///
  /// 0       16      32     48     64
  /// |-------|-------|------|------| (buffer)
  ///           [       ]             (bitmap)
  ///         |-------|------|        (returned words)
  ///
  /// \warning The words may contain bytes which lie outside the buffer or are
  /// uninitialized.
  template <typename Word>
  View<Word> words() const {
    auto bytes_addr = reinterpret_cast<intptr_t>(bytes().data());
    auto words_addr = bytes_addr - bytes_addr % sizeof(Word);
    auto word_byte_count =
        BitUtil::RoundUpToPowerOf2(static_cast<int64_t>(bytes_addr + bytes().size()),
                                   static_cast<int64_t>(sizeof(Word))) -
        words_addr;
    return View<Word>(reinterpret_cast<const Word*>(words_addr),
                      word_byte_count / sizeof(Word));
  }

  /// offset of first bit relative to words<Word>().data()
  template <typename Word>
  int64_t word_offset() const {
    return offset_ + 8 * (reinterpret_cast<intptr_t>(buffer_->data()) -
                          reinterpret_cast<intptr_t>(words<Word>().data()));
  }

  /// load words from bitmaps bitwise
  template <size_t N, typename Word>
  static void SafeLoadWords(const Bitmap (&bitmaps)[N], int64_t offset,
                            int64_t out_length, bool set_trailing_bits,
                            std::array<Word, N>* out) {
    out->fill(0);

    int64_t out_offset = set_trailing_bits ? sizeof(Word) * 8 - out_length : 0;

    Bitmap slices[N], out_bitmaps[N];
    for (size_t i = 0; i < N; ++i) {
      slices[i] = bitmaps[i].Slice(offset, out_length);
      out_bitmaps[i] = Bitmap(&out->at(i), out_offset, out_length);
    }

    int64_t bit_i = 0;
    Bitmap::VisitBits(slices, [&](std::bitset<N> bits) {
      for (size_t i = 0; i < N; ++i) {
        out_bitmaps[i].SetBitTo(bit_i, bits[i]);
      }
      ++bit_i;
    });
  }

  std::shared_ptr<BooleanArray> ToArray() const;

  /// assert bitmaps have identical length and return that length
  static int64_t BitLength(const Bitmap* bitmaps, size_t N);

  std::shared_ptr<Buffer> buffer_;
  int64_t offset_ = 0, length_ = 0;
};

/// \brief Store a stack of bitsets efficiently. The top bitset may be
/// accessed and its bits may be modified, but it may not be resized.
class BitsetStack {
 public:
  using reference = typename std::vector<bool>::reference;

  /// \brief push a bitset onto the stack
  /// \param size number of bits in the next bitset
  /// \param value initial value for bits in the pushed bitset
  void Push(int size, bool value) {
    offsets_.push_back(bit_count());
    bits_.resize(bit_count() + size, value);
  }

  /// \brief number of bits in the bitset at the top of the stack
  int TopSize() const {
    if (offsets_.size() == 0) return 0;
    return bit_count() - offsets_.back();
  }

  /// \brief pop a bitset off the stack
  void Pop() {
    bits_.resize(offsets_.back());
    offsets_.pop_back();
  }

  /// \brief get the value of a bit in the top bitset
  /// \param i index of the bit to access
  bool operator[](int i) const { return bits_[offsets_.back() + i]; }

  /// \brief get a mutable reference to a bit in the top bitset
  /// \param i index of the bit to access
  reference operator[](int i) { return bits_[offsets_.back() + i]; }

 private:
  int bit_count() const { return static_cast<int>(bits_.size()); }
  std::vector<bool> bits_;
  std::vector<int> offsets_;
};

}  // namespace internal
}  // namespace arrow
