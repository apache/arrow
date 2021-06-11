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
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/compare.h"
#include "arrow/util/endian.h"
#include "arrow/util/functional.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class BooleanArray;

namespace internal {

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

  void Stride(int64_t stride) {
    this->offset_ += stride;
    this->length_ -= stride;
  }

  std::string ToString() const;

  bool Equals(const Bitmap& other) const;

  std::string Diff(const Bitmap& other) const;

  bool GetBit(int64_t i) const { return BitUtil::GetBit(buffer_->data(), i + offset_); }

  bool operator[](int64_t i) const { return GetBit(i); }

  void SetBitTo(int64_t i, bool v) const {
    BitUtil::SetBitTo(buffer_->mutable_data(), i + offset_, v);
  }

  void SetBitsTo(bool v) {
    BitUtil::SetBitsTo(buffer_->mutable_data(), offset_, length_, v);
  }

  void CopyFrom(const Bitmap& other);
  void CopyFromInverted(const Bitmap& other);

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

  /// \brief Visit bits from each bitmap as bitset<N>
  ///
  /// All bitmaps must have identical length.
  template <size_t N, typename Visitor>
  static void VisitBits(const std::array<Bitmap, N>& bitmaps, Visitor&& visitor) {
    int64_t bit_length = BitLength(bitmaps);
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
  // NOTE: this function is efficient on 3+ sufficiently large bitmaps.
  // It also has a large prolog / epilog overhead and should be used
  // carefully in other cases.
  // For 2 bitmaps or less, and/or smaller bitmaps, see also VisitTwoBitBlocksVoid
  // and BitmapUInt64Reader.
  template <size_t N, typename Visitor,
            typename Word = typename std::decay<
                internal::call_traits::argument_type<0, Visitor&&>>::type::value_type>
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
            auto words0 = BitUtil::ToLittleEndian(words[i][word_i]);
            auto words1 = BitUtil::ToLittleEndian(words[i][word_i + 1]);
            visited_words[i] = BitUtil::FromLittleEndian(
                (words0 >> offsets[i]) | (words1 << (kBitWidth - offsets[i])));
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

  /// \brief Visit words of bits from each bitmap as array<Word, N>
  ///
  /// All bitmaps must have identical length. The first bit in a visited bitmap
  /// may be offset within the first visited word, but words will otherwise contain
  /// densely packed bits loaded from the bitmap. That offset within the first word is
  /// returned.
  ///
  /// TODO(bkietz) allow for early termination
  // NOTE: this function is efficient on 3+ sufficiently large bitmaps.
  // It also has a large prolog / epilog overhead and should be used
  // carefully in other cases.
  // For 2 bitmaps or less, and/or smaller bitmaps, see also VisitTwoBitBlocksVoid
  // and BitmapUInt64Reader.
  template <size_t N, typename Visitor,
            typename Word = typename std::decay<
                internal::call_traits::argument_type<0, Visitor&&>>::type::value_type>
  static int64_t VisitWordsAndWrite(const std::array<Bitmap, N>& bitmaps_arg,
                                    Visitor&& visitor, Bitmap* out_bitmap_arg) {
    constexpr int64_t kBitWidth = sizeof(Word) * 8;

    // local, mutable variables which will be sliced/decremented to represent consumption:
    Bitmap bitmaps[N];  // todo use std::array here
    int64_t bit_length = BitLength(bitmaps_arg);

    struct BitmapHolder {
      BitmapHolder() = default;
      explicit BitmapHolder(Bitmap* bitmap_)
          : bitmap(bitmap_),
            word_offset(bitmap_->template word_offset<Word>()),
            words(bitmap_->template words<Word>()) {
        assert(BitmapHolder::word_offset >= 0 && BitmapHolder::word_offset < kBitWidth);
      }

      inline void StrideAndUpdate(int64_t _stride) {
        BitmapHolder::bitmap->Stride(_stride);
        BitmapHolder::word_offset = bitmap->template word_offset<Word>();
        assert(BitmapHolder::word_offset >= 0 && BitmapHolder::word_offset < kBitWidth);
        BitmapHolder::words = bitmap->template words<Word>();
      }

      Bitmap* bitmap{};
      int64_t word_offset = 0;
      View<Word> words;
    };

    std::array<BitmapHolder, N> in_bitmaps;
    Bitmap out_bitmap = *out_bitmap_arg;  // make a copy

    for (size_t i = 0; i < N; ++i) {
      bitmaps[i] = bitmaps_arg[i];  // make a copy
      in_bitmaps[i] = BitmapHolder(&bitmaps[i]);
    }

    auto consume = [&](int64_t consumed_bits) {
      for (size_t i = 0; i < N; ++i) {
        in_bitmaps[i].StrideAndUpdate(consumed_bits);
      }
      out_bitmap.Stride(consumed_bits);

      bit_length -= consumed_bits;
    };

    std::array<Word, N> visited_words;
    visited_words.fill(0);

    if (bit_length <= kBitWidth * 2) {
      // bitmaps fit into one or two words so don't bother with optimization
      while (bit_length > 0) {
        auto leading_bits = std::min(bit_length, kBitWidth);
        SafeLoadWords(bitmaps, 0, leading_bits, false, &visited_words);
        Word visit_out = visitor(visited_words);  // outputs a word/ partial word
        CopyBitmap(reinterpret_cast<uint8_t*>(&visit_out), 0, leading_bits,
                   out_bitmap.buffer_->mutable_data(), out_bitmap.offset());
        consume(leading_bits);
      }
      return 0;
    }

    auto word_offset_comp = [](const BitmapHolder& l, const BitmapHolder& r) {
      return l.word_offset < r.word_offset;
    };

    int64_t max_word_offset =
        (*std::max_element(in_bitmaps.begin(), in_bitmaps.end(), word_offset_comp))
            .word_offset;
    int64_t min_word_offset =
        (*std::min_element(in_bitmaps.begin(), in_bitmaps.end(), word_offset_comp))
            .word_offset;
    if (max_word_offset > 0) {
      // consume leading bits
      auto leading_bits = kBitWidth - min_word_offset;
      SafeLoadWords(bitmaps, 0, leading_bits, true, &visited_words);
      Word visit_out = visitor(visited_words);
      CopyBitmap(reinterpret_cast<uint8_t*>(&visit_out), sizeof(Word) * 8 - leading_bits,
                 leading_bits, out_bitmap.buffer_->mutable_data(), out_bitmap.offset());
      consume(leading_bits);
    }
    assert((*std::min_element(in_bitmaps.begin(), in_bitmaps.end(), word_offset_comp))
               .word_offset == 0);

    int64_t whole_word_count = bit_length / kBitWidth;
    assert(whole_word_count >= 1);

    std::vector<Word> visit_outs;
    visit_outs.reserve(whole_word_count);

    if (min_word_offset == max_word_offset) {
      // all offsets were identical, all leading bits have been consumed
      assert(std::all_of(
          in_bitmaps.begin(), in_bitmaps.end(),
          [](const BitmapHolder& holder) { return holder.word_offset == 0; }));

      for (int64_t word_i = 0; word_i < whole_word_count; ++word_i) {
        for (size_t i = 0; i < N; ++i) {
          visited_words[i] = in_bitmaps[i].words[word_i];
        }
        visit_outs.template emplace_back(visitor(visited_words));
      }
      CopyBitmap(reinterpret_cast<const uint8_t*>(visit_outs.data()), 0,
                 whole_word_count * kBitWidth, out_bitmap.buffer_->mutable_data(),
                 out_bitmap.offset());
      consume(whole_word_count * kBitWidth);
    } else {
      // leading bits from potentially incomplete words have been consumed

      // word_i such that words[i][word_i] and words[i][word_i + 1] are lie entirely
      // within the bitmap for all i
      for (int64_t word_i = 0; word_i < whole_word_count - 1; ++word_i) {
        for (size_t i = 0; i < N; ++i) {
          const auto ith_words = in_bitmaps[i].words;
          const auto ith_word_offset = in_bitmaps[i].word_offset;
          if (ith_word_offset == 0) {
            visited_words[i] = ith_words[word_i];
          } else {
            auto words0 = BitUtil::ToLittleEndian(ith_words[word_i]);
            auto words1 = BitUtil::ToLittleEndian(ith_words[word_i + 1]);
            visited_words[i] = BitUtil::FromLittleEndian(
                (words0 >> ith_word_offset) | (words1 << (kBitWidth - ith_word_offset)));
          }
        }
        visit_outs.template emplace_back(visitor(visited_words));
      }
      CopyBitmap(reinterpret_cast<const uint8_t*>(visit_outs.data()), 0,
                 (whole_word_count - 1) * kBitWidth, out_bitmap.buffer_->mutable_data(),
                 out_bitmap.offset());
      consume((whole_word_count - 1) * kBitWidth);

      SafeLoadWords(bitmaps, 0, kBitWidth, false, &visited_words);

      Word visit_out = visitor(visited_words);  // outputs a word/ partial word
      CopyBitmap(reinterpret_cast<uint8_t*>(&visit_out), 0, kBitWidth,
                 out_bitmap.buffer_->mutable_data(), out_bitmap.offset());
      consume(kBitWidth);
    }

    // load remaining bits
    if (bit_length > 0) {
      SafeLoadWords(bitmaps, 0, bit_length, false, &visited_words);
      Word visit_out = visitor(visited_words);
      CopyBitmap(reinterpret_cast<uint8_t*>(&visit_out), 0, bit_length,
                 out_bitmap.buffer_->mutable_data(), out_bitmap.offset());
    }

    return min_word_offset;
  }

  template <size_t N, size_t M, typename Word>
  using MultiOutputVisitor = std::function<void(const std::array<Word, N>& in_words,
                                                std::array<Word, M>& out_words)>;

  template <size_t N, size_t M, typename Word>
  static void VisitWordsAndWrite(const std::array<Bitmap, N>& bitmaps_arg,
                                 MultiOutputVisitor<N, M, Word>&& visitor,
                                 std::array<Bitmap, M>& out_bitmaps_arg) {
    constexpr int64_t kBitWidth = sizeof(Word) * 8;

    int64_t bit_length = BitLength(bitmaps_arg);
    assert(bit_length == BitLength(out_bitmaps_arg));

    std::array<BitmapWordReader<Word>, N> readers;
    for (size_t i = 0; i < N; ++i) {
      readers[i] = BitmapWordReader<Word>(bitmaps_arg[i].buffer_->data(),
                                          bitmaps_arg[i].offset_, bitmaps_arg[i].length_);
    }

    std::array<BitmapWordWriter<Word>, M> writers;
    for (size_t i = 0; i < M; ++i) {
      writers[i] =
          BitmapWordWriter<Word>(out_bitmaps_arg[i].buffer_->mutable_data(),
                                 out_bitmaps_arg[i].offset_, out_bitmaps_arg[i].length_);
    }

    std::array<Word, N> visited_words;
    visited_words.fill(0);
    std::array<Word, M> output_words;
    output_words.fill(0);

    // every reader will have same number of words, since they are same length'ed
    // todo this will be inefficient in some cases. When there are offsets beyond Word
    //  boundary, every Word would have to be created from 2 adjoining Words
    auto n_words = readers[0].words();
    while (n_words--) {
      // first collect all words to visited_words array
      for (size_t i = 0; i < N; i++) {
        visited_words[i] = readers[i].NextWord();
      }

      visitor(visited_words, output_words);

      for (size_t i = 0; i < M; i++) {
        writers[i].PutNextWord(output_words[i]);
      }

      bit_length -= kBitWidth;
    }

    // every reader will have same number of trailing bytes, because of the above reason
    // todo when the above issue is resolved, following logic also needs to be fixed!
    // tailing portion could be more than one word! (ref: BitmapWordReader constructor)
    assert(static_cast<size_t>(bit_length) < kBitWidth * 2);
    if (bit_length / kBitWidth) {
      // there's one full word in trailing portion. Cant use NextWord() here because it
      // doesn't stride the trailing metadata
      for (size_t i = 0; i < N; i++) {
        visited_words[i] = 0;
        for (size_t b = 0; b < sizeof(Word); b++) {
          int dummy;
          auto byte = static_cast<Word>(readers[i].NextTrailingByte(dummy));
          visited_words[i] |= byte << (b * 8);
        }
      }

      visitor(visited_words, output_words);

      for (size_t i = 0; i < M; i++) {
        writers[i].PutNextWord(output_words[i]);
      }

      bit_length -= kBitWidth;
    }

    // clean-up last partial word
    if (bit_length) {
      output_words.fill(0);
      for (size_t i = 0; i < N; i++) {
        visited_words[i] = 0;
        int n_byte = readers[i].trailing_bytes();
        for (int b = 0; b < n_byte; b++) {
          int valid_bits;
          auto byte = static_cast<Word>(readers[i].NextTrailingByte(valid_bits));
          visited_words[i] |= (byte << b * 8);
        }
      }

      visitor(visited_words, output_words);

      for (size_t i = 0; i < M; i++) {
        writers[i].PutNextWord(output_words[i], bit_length);
      }
    }
  }

  template <size_t N, typename Word>
  using SingleOutputVisitor =
      std::function<void(const std::array<Word, N>& in_words, Word& out_words)>;

  template <size_t N, typename Word>
  static void VisitWordsAndWrite(const std::array<Bitmap, N>& bitmaps_arg,
                                 SingleOutputVisitor<N, Word>&& visitor,
                                 Bitmap& out_bitmap_arg) {
    std::array<Bitmap, 1> out_bitmaps{out_bitmap_arg};
    VisitWordsAndWrite<N, 1, Word>(
        bitmaps_arg,
        [&](const std::array<Word, N>& in_words, std::array<Word, 1>& out_words) {
          visitor(in_words, out_words[0]);
        },
        out_bitmaps);
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

  template <size_t N>
  static int64_t BitLength(const std::array<Bitmap, N>& bitmaps) {
    for (size_t i = 1; i < N; ++i) {
      assert(bitmaps[i].length() == bitmaps[0].length());
    }
    return bitmaps[0].length();
  }

  std::shared_ptr<Buffer> buffer_;
  int64_t offset_ = 0, length_ = 0;
};

}  // namespace internal
}  // namespace arrow
