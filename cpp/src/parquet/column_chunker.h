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

#include <string>
#include <vector>
#include "arrow/array.h"
#include "parquet/level_conversion.h"

using arrow::internal::checked_cast;

namespace parquet {
namespace internal {

// Constants
const uint64_t GEAR_HASH_TABLE[] = {
    0xb088d3a9e840f559, 0x5652c7f739ed20d6, 0x45b28969898972ab, 0x6b0a89d5b68ec777,
    0x368f573e8b7a31b7, 0x1dc636dce936d94b, 0x207a4c4e5554d5b6, 0xa474b34628239acb,
    0x3b06a83e1ca3b912, 0x90e78d6c2f02baf7, 0xe1c92df7150d9a8a, 0x8e95053a1086d3ad,
    0x5a2ef4f1b83a0722, 0xa50fac949f807fae, 0x0e7303eb80d8d681, 0x99b07edc1570ad0f,
    0x689d2fb555fd3076, 0x00005082119ea468, 0xc4b08306a88fcc28, 0x3eb0678af6374afd,
    0xf19f87ab86ad7436, 0xf2129fbfbe6bc736, 0x481149575c98a4ed, 0x0000010695477bc5,
    0x1fba37801a9ceacc, 0x3bf06fd663a49b6d, 0x99687e9782e3874b, 0x79a10673aa50d8e3,
    0xe4accf9e6211f420, 0x2520e71f87579071, 0x2bd5d3fd781a8a9b, 0x00de4dcddd11c873,
    0xeaa9311c5a87392f, 0xdb748eb617bc40ff, 0xaf579a8df620bf6f, 0x86a6e5da1b09c2b1,
    0xcc2fc30ac322a12e, 0x355e2afec1f74267, 0x2d99c8f4c021a47b, 0xbade4b4a9404cfc3,
    0xf7b518721d707d69, 0x3286b6587bf32c20, 0x0000b68886af270c, 0xa115d6e4db8a9079,
    0x484f7e9c97b2e199, 0xccca7bb75713e301, 0xbf2584a62bb0f160, 0xade7e813625dbcc8,
    0x000070940d87955a, 0x8ae69108139e626f, 0xbd776ad72fde38a2, 0xfb6b001fc2fcc0cf,
    0xc7a474b8e67bc427, 0xbaf6f11610eb5d58, 0x09cb1f5b6de770d1, 0xb0b219e6977d4c47,
    0x00ccbc386ea7ad4a, 0xcc849d0adf973f01, 0x73a3ef7d016af770, 0xc807d2d386bdbdfe,
    0x7f2ac9966c791730, 0xd037a86bc6c504da, 0xf3f17c661eaa609d, 0xaca626b04daae687,
    0x755a99374f4a5b07, 0x90837ee65b2caede, 0x6ee8ad93fd560785, 0x0000d9e11053edd8,
    0x9e063bb2d21cdbd7, 0x07ab77f12a01d2b2, 0xec550255e6641b44, 0x78fb94a8449c14c6,
    0xc7510e1bc6c0f5f5, 0x0000320b36e4cae3, 0x827c33262c8b1a2d, 0x14675f0b48ea4144,
    0x267bd3a6498deceb, 0xf1916ff982f5035e, 0x86221b7ff434fb88, 0x9dbecee7386f49d8,
    0xea58f8cac80f8f4a, 0x008d198692fc64d8, 0x6d38704fbabf9a36, 0xe032cb07d1e7be4c,
    0x228d21f6ad450890, 0x635cb1bfc02589a5, 0x4620a1739ca2ce71, 0xa7e7dfe3aae5fb58,
    0x0c10ca932b3c0deb, 0x2727fee884afed7b, 0xa2df1c6df9e2ab1f, 0x4dcdd1ac0774f523,
    0x000070ffad33e24e, 0xa2ace87bc5977816, 0x9892275ab4286049, 0xc2861181ddf18959,
    0xbb9972a042483e19, 0xef70cd3766513078, 0x00000513abfc9864, 0xc058b61858c94083,
    0x09e850859725e0de, 0x9197fb3bf83e7d94, 0x7e1e626d12b64bce, 0x520c54507f7b57d1,
    0xbee1797174e22416, 0x6fd9ac3222e95587, 0x0023957c9adfbf3e, 0xa01c7d7e234bbe15,
    0xaba2c758b8a38cbb, 0x0d1fa0ceec3e2b30, 0x0bb6a58b7e60b991, 0x4333dd5b9fa26635,
    0xc2fd3b7d4001c1a3, 0xfb41802454731127, 0x65a56185a50d18cb, 0xf67a02bd8784b54f,
    0x696f11dd67e65063, 0x00002022fca814ab, 0x8cd6be912db9d852, 0x695189b6e9ae8a57,
    0xee9453b50ada0c28, 0xd8fc5ea91a78845e, 0xab86bf191a4aa767, 0x0000c6b5c86415e5,
    0x267310178e08a22e, 0xed2d101b078bca25, 0x3b41ed84b226a8fb, 0x13e622120f28dc06,
    0xa315f5ebfb706d26, 0x8816c34e3301bace, 0xe9395b9cbb71fdae, 0x002ce9202e721648,
    0x4283db1d2bb3c91c, 0xd77d461ad2b1a6a5, 0xe2ec17e46eeb866b, 0xb8e0be4039fbc47c,
    0xdea160c4d5299d04, 0x7eec86c8d28c3634, 0x2119ad129f98a399, 0xa6ccf46b61a283ef,
    0x2c52cedef658c617, 0x2db4871169acdd83, 0x0000f0d6f39ecbe9, 0x3dd5d8c98d2f9489,
    0x8a1872a22b01f584, 0xf282a4c40e7b3cf2, 0x8020ec2ccb1ba196, 0x6693b6e09e59e313,
    0x0000ce19cc7c83eb, 0x20cb5735f6479c3b, 0x762ebf3759d75a5b, 0x207bfe823d693975,
    0xd77dc112339cd9d5, 0x9ba7834284627d03, 0x217dc513e95f51e9, 0xb27b1a29fc5e7816,
    0x00d5cd9831bb662d, 0x71e39b806d75734c, 0x7e572af006fb1a23, 0xa2734f2f6ae91f85,
    0xbf82c6b5022cddf2, 0x5c3beac60761a0de, 0xcdc893bb47416998, 0x6d1085615c187e01,
    0x77f8ae30ac277c5d, 0x917c6b81122a2c91, 0x5b75b699add16967, 0x0000cf6ae79a069b,
    0xf3c40afa60de1104, 0x2063127aa59167c3, 0x621de62269d1894d, 0xd188ac1de62b4726,
    0x107036e2154b673c, 0x0000b85f28553a1d, 0xf2ef4e4c18236f3d, 0xd9d6de6611b9f602,
    0xa1fc7955fb47911c, 0xeb85fd032f298dbd, 0xbe27502fb3befae1, 0xe3034251c4cd661e,
    0x441364d354071836, 0x0082b36c75f2983e, 0xb145910316fa66f0, 0x021c069c9847caf7,
    0x2910dfc75a4b5221, 0x735b353e1c57a8b5, 0xce44312ce98ed96c, 0xbc942e4506bdfa65,
    0xf05086a71257941b, 0xfec3b215d351cead, 0x00ae1055e0144202, 0xf54b40846f42e454,
    0x00007fd9c8bcbcc8, 0xbfbd9ef317de9bfe, 0xa804302ff2854e12, 0x39ce4957a5e5d8d4,
    0xffb9e2a45637ba84, 0x55b9ad1d9ea0818b, 0x00008acbf319178a, 0x48e2bfc8d0fbfb38,
    0x8be39841e848b5e8, 0x0e2712160696a08b, 0xd51096e84b44242a, 0x1101ba176792e13a,
    0xc22e770f4531689d, 0x1689eff272bbc56c, 0x00a92a197f5650ec, 0xbc765990bda1784e,
    0xc61441e392fcb8ae, 0x07e13a2ced31e4a0, 0x92cbe984234e9d4d, 0x8f4ff572bb7d8ac5,
    0x0b9670c00b963bd0, 0x62955a581a03eb01, 0x645f83e5ea000254, 0x41fce516cd88f299,
    0xbbda9748da7a98cf, 0x0000aab2fe4845fa, 0x19761b069bf56555, 0x8b8f5e8343b6ad56,
    0x3e5d1cfd144821d9, 0xec5c1e2ca2b0cd8f, 0xfaf7e0fea7fbb57f, 0x000000d3ba12961b,
    0xda3f90178401b18e, 0x70ff906de33a5feb, 0x0527d5a7c06970e7, 0x22d8e773607c13e9,
    0xc9ab70df643c3bac, 0xeda4c6dc8abe12e3, 0xecef1f410033e78a, 0x0024c2b274ac72cb,
    0x06740d954fa900b4, 0x1d7a299b323d6304, 0xb3c37cb298cbead5, 0xc986e3c76178739b,
    0x9fabea364b46f58a, 0x6da214c5af85cc56, 0x17a43ed8b7a38f84, 0x6eccec511d9adbeb,
    0xf9cab30913335afb, 0x4a5e60c5f415eed2, 0x00006967503672b4, 0x9da51d121454bb87,
    0x84321e13b9bbc816, 0xfb3d6fb6ab2fdd8d, 0x60305eed8e160a8d, 0xcbbf4b14e9946ce8,
    0x00004f63381b10c3, 0x07d5b7816fcc4e10, 0xe5a536726a6a8155, 0x57afb23447a07fdd,
    0x18f346f7abc9d394, 0x636dc655d61ad33d, 0xcc8bab4939f7f3f6, 0x63c7a906c1dd187b};

const uint64_t MASK = 0xffff00000000000;
// const int MIN_LEN = 65536 / 8;
// const int MAX_LEN = 65536 * 2;
const int64_t MIN_LEN = 256 * 1024;
const int64_t MAX_LEN = 2 * 1024 * 1024;

// create a fake null array class with a GetView method returning 0 always
class FakeNullArray {
 public:
  uint8_t GetView(int64_t i) const { return 0; }

  std::shared_ptr<::arrow::DataType> type() const { return ::arrow::null(); }

  int64_t null_count() const { return 0; }
};

class GearHash {
 public:
  GearHash(const LevelInfo& level_info, uint64_t mask, uint64_t min_len, uint64_t max_len)
      : level_info_(level_info),
        mask_(mask == 0 ? MASK : mask),
        min_len_(min_len == 0 ? MIN_LEN : min_len),
        max_len_(max_len == 0 ? MAX_LEN : max_len) {}

  template <typename T>
  bool Roll(const T value) {
    constexpr size_t BYTE_WIDTH = sizeof(T);
    chunk_size_ += BYTE_WIDTH;
    // if (chunk_size_ < min_len_) {
    //   return false;
    // }
    auto bytes = reinterpret_cast<const uint8_t*>(&value);
    bool match = false;
#pragma unroll
    for (size_t i = 0; i < BYTE_WIDTH; ++i) {
      hash_ = (hash_ << 1) + GEAR_HASH_TABLE[bytes[i]];
      if ((hash_ & mask_) == 0) {
        match = true;
      }
    }
    return match;
  }

  bool Roll(std::string_view value) {
    chunk_size_ += value.size();
    // if (chunk_size_ < min_len_) {
    //   return false;
    // }
    bool match = false;
    for (char c : value) {
      hash_ = (hash_ << 1) + GEAR_HASH_TABLE[static_cast<uint8_t>(c)];
      if ((hash_ & mask_) == 0) {
        match = true;
      }
    }
    return match;
  }

  bool Check(bool match) {
    if ((match && (chunk_size_ >= min_len_)) || (chunk_size_ >= max_len_)) {
      chunk_size_ = 0;
      return true;
    } else {
      return false;
    }
  }

  // bool Check(bool match) {
  //   if ((match && (chunk_size_ >= min_len_)) || (chunk_size_ >= max_len_)) {
  //     chunk_size_ = 0;
  //     return true;
  //   } else {
  //     return false;
  //   }
  // }

  // template <typename T>
  // const std::vector<std::tuple<int64_t, int64_t, int64_t>> GetBoundaries(
  //     int64_t num_levels, const T& leaf_array) {
  //   std::vector<std::tuple<int64_t, int64_t, int64_t>> result;

  //   int64_t offset = 0;
  //   int64_t prev_offset = 0;

  //   while (offset < num_levels) {
  //     if (Check(Roll(leaf_array.GetView(offset)))) {
  //       result.push_back(std::make_tuple(prev_offset, prev_offset, offset -
  //       prev_offset)); prev_offset = offset;
  //     }
  //     ++offset;
  //   }
  //   if (prev_offset < num_levels) {
  //     result.push_back(std::make_tuple(prev_offset, prev_offset, num_levels -
  //     prev_offset));
  //   }
  //   return result;
  // }

  template <typename T>
  const std::vector<std::tuple<int64_t, int64_t, int64_t>> GetBoundaries(
      const int16_t* def_levels, const int16_t* rep_levels, int64_t num_levels,
      const T& leaf_array) {
    std::vector<std::tuple<int64_t, int64_t, int64_t>> result;
    bool has_def_levels = level_info_.def_level > 0;
    bool has_rep_levels = level_info_.rep_level > 0;
    // bool no_nulls = leaf_array.null_count() == 0;
    // if (!has_rep_levels && !maybe_parent_nulls && no_nulls) {
    //   return GetBoundaries(num_levels, leaf_array);
    // }

    bool def_match, rep_match, val_match;
    int64_t level_offset = 0;
    int64_t value_offset = 0;
    int64_t record_level_offset = 0;
    int64_t record_value_offset = 0;
    int64_t prev_record_level_offset = 0;
    int64_t prev_record_value_offset = 0;

    while (level_offset < num_levels) {
      int16_t def_level = has_def_levels ? def_levels[level_offset] : 0;
      int16_t rep_level = has_rep_levels ? rep_levels[level_offset] : 0;

      if (rep_level == 0) {
        // record boundary
        record_level_offset = level_offset;
        record_value_offset = value_offset;
      }

      def_match = Roll(def_level);
      rep_match = Roll(rep_level);
      ++level_offset;

      if (has_rep_levels) {
        if (def_level >= level_info_.repeated_ancestor_def_level) {
          val_match = Roll(leaf_array.GetView(value_offset));
          ++value_offset;
        } else {
          val_match = false;
        }
      } else {
        val_match = Roll(leaf_array.GetView(value_offset));
        ++value_offset;
      }

      if (Check(def_match || rep_match || val_match)) {
        auto levels_to_write = record_level_offset - prev_record_level_offset;
        if (levels_to_write > 0) {
          result.emplace_back(prev_record_level_offset, prev_record_value_offset,
                              levels_to_write);
          prev_record_level_offset = record_level_offset;
          prev_record_value_offset = record_value_offset;
        }
      }
    }

    auto levels_to_write = num_levels - prev_record_level_offset;
    if (levels_to_write > 0) {
      result.emplace_back(prev_record_level_offset, prev_record_value_offset,
                          levels_to_write);
    }
    return result;
  }

#define PRIMITIVE_CASE(TYPE_ID, ArrowType)                   \
  case ::arrow::Type::TYPE_ID:                               \
    return GetBoundaries(def_levels, rep_levels, num_levels, \
                         checked_cast<const ::arrow::ArrowType##Array&>(values));

  const ::arrow::Result<std::vector<std::tuple<int64_t, int64_t, int64_t>>> GetBoundaries(
      const int16_t* def_levels, const int16_t* rep_levels, int64_t num_levels,
      const ::arrow::Array& values) {
    auto type_id = values.type()->id();
    switch (type_id) {
      PRIMITIVE_CASE(BOOL, Boolean)
      PRIMITIVE_CASE(INT8, Int8)
      PRIMITIVE_CASE(INT16, Int16)
      PRIMITIVE_CASE(INT32, Int32)
      PRIMITIVE_CASE(INT64, Int64)
      PRIMITIVE_CASE(UINT8, UInt8)
      PRIMITIVE_CASE(UINT16, UInt16)
      PRIMITIVE_CASE(UINT32, UInt32)
      PRIMITIVE_CASE(UINT64, UInt64)
      PRIMITIVE_CASE(HALF_FLOAT, HalfFloat)
      PRIMITIVE_CASE(FLOAT, Float)
      PRIMITIVE_CASE(DOUBLE, Double)
      PRIMITIVE_CASE(STRING, String)
      PRIMITIVE_CASE(BINARY, Binary)
      PRIMITIVE_CASE(FIXED_SIZE_BINARY, FixedSizeBinary)
      PRIMITIVE_CASE(DATE32, Date32)
      PRIMITIVE_CASE(DATE64, Date64)
      PRIMITIVE_CASE(TIME32, Time32)
      PRIMITIVE_CASE(TIME64, Time64)
      PRIMITIVE_CASE(TIMESTAMP, Timestamp)
      PRIMITIVE_CASE(DURATION, Duration)
      PRIMITIVE_CASE(DECIMAL128, Decimal128)
      PRIMITIVE_CASE(DECIMAL256, Decimal256)
      case ::arrow::Type::DICTIONARY:
        return GetBoundaries(
            def_levels, rep_levels, num_levels,
            *checked_cast<const ::arrow::DictionaryArray&>(values).indices());
      case ::arrow::Type::NA:
        FakeNullArray fake_null_array;
        return GetBoundaries(def_levels, rep_levels, num_levels, fake_null_array);
      default:
        return ::arrow::Status::NotImplemented("Unsupported type " +
                                               values.type()->ToString());
    }
  }

 private:
  const internal::LevelInfo& level_info_;
  uint64_t mask_ = MASK;
  uint64_t min_len_;
  uint64_t max_len_;
  uint64_t hash_ = 0;
  uint64_t chunk_size_ = 0;
};

}  // namespace internal
}  // namespace parquet
