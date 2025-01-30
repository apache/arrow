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

const uint64_t GEAR_TABLE[256] = {
    0x3b5d3c7d207e37dc, 0x784d68ba91123086, 0xcd52880f882e7298, 0xeacf8e4e19fdcca7,
    0xc31f385dfbd1632b, 0x1d5f27001e25abe6, 0x83130bde3c9ad991, 0xc4b225676e9b7649,
    0xaa329b29e08eb499, 0xb67fcbd21e577d58, 0x0027baaada2acf6b, 0xe3ef2d5ac73c2226,
    0x0890f24d6ed312b7, 0xa809e036851d7c7e, 0xf0a6fe5e0013d81b, 0x1d026304452cec14,
    0x03864632648e248f, 0xcdaacf3dcd92b9b4, 0xf5e012e63c187856, 0x8862f9d3821c00b6,
    0xa82f7338750f6f8a, 0x1e583dc6c1cb0b6f, 0x7a3145b69743a7f1, 0xabb20fee404807eb,
    0xb14b3cfe07b83a5d, 0xb9dc27898adb9a0f, 0x3703f5e91baa62be, 0xcf0bb866815f7d98,
    0x3d9867c41ea9dcd3, 0x1be1fa65442bf22c, 0x14300da4c55631d9, 0xe698e9cbc6545c99,
    0x4763107ec64e92a5, 0xc65821fc65696a24, 0x76196c064822f0b7, 0x485be841f3525e01,
    0xf652bc9c85974ff5, 0xcad8352face9e3e9, 0x2a6ed1dceb35e98e, 0xc6f483badc11680f,
    0x3cfd8c17e9cf12f1, 0x89b83c5e2ea56471, 0xae665cfd24e392a9, 0xec33c4e504cb8915,
    0x3fb9b15fc9fe7451, 0xd7fd1fd1945f2195, 0x31ade0853443efd8, 0x255efc9863e1e2d2,
    0x10eab6008d5642cf, 0x46f04863257ac804, 0xa52dc42a789a27d3, 0xdaaadf9ce77af565,
    0x6b479cd53d87febb, 0x6309e2d3f93db72f, 0xc5738ffbaa1ff9d6, 0x6bd57f3f25af7968,
    0x67605486d90d0a4a, 0xe14d0b9663bfbdae, 0xb7bbd8d816eb0414, 0xdef8a4f16b35a116,
    0xe7932d85aaaffed6, 0x08161cbae90cfd48, 0x855507beb294f08b, 0x91234ea6ffd399b2,
    0xad70cf4b2435f302, 0xd289a97565bc2d27, 0x8e558437ffca99de, 0x96d2704b7115c040,
    0x0889bbcdfc660e41, 0x5e0d4e67dc92128d, 0x72a9f8917063ed97, 0x438b69d409e016e3,
    0xdf4fed8a5d8a4397, 0x00f41dcf41d403f7, 0x4814eb038e52603f, 0x9dafbacc58e2d651,
    0xfe2f458e4be170af, 0x4457ec414df6a940, 0x06e62f1451123314, 0xbd1014d173ba92cc,
    0xdef318e25ed57760, 0x9fea0de9dfca8525, 0x459de1e76c20624b, 0xaeec189617e2d666,
    0x126a2c06ab5a83cb, 0xb1321532360f6132, 0x65421503dbb40123, 0x2d67c287ea089ab3,
    0x6c93bff5a56bd6b6, 0x4ffb2036cab6d98d, 0xce7b785b1be7ad4f, 0xedb42ef6189fd163,
    0xdc905288703988f6, 0x365f9c1d2c691884, 0xc640583680d99bfe, 0x3cd4624c07593ec6,
    0x7f1ea8d85d7c5805, 0x014842d480b57149, 0x0b649bcb5a828688, 0xbcd5708ed79b18f0,
    0xe987c862fbd2f2f0, 0x982731671f0cd82c, 0xbaf13e8b16d8c063, 0x8ea3109cbd951bba,
    0xd141045bfb385cad, 0x2acbc1a0af1f7d30, 0xe6444d89df03bfdf, 0xa18cc771b8188ff9,
    0x9834429db01c39bb, 0x214add07fe086a1f, 0x8f07c19b1f6b3ff9, 0x56a297b1bf4ffe55,
    0x94d558e493c54fc7, 0x40bfc24c764552cb, 0x931a706f8a8520cb, 0x32229d322935bd52,
    0x2560d0f5dc4fefaf, 0x9dbcc48355969bb6, 0x0fd81c3985c0b56a, 0xe03817e1560f2bda,
    0xc1bb4f81d892b2d5, 0xb0c4864f4e28d2d7, 0x3ecc49f9d9d6c263, 0x51307e99b52ba65e,
    0x8af2b688da84a752, 0xf5d72523b91b20b6, 0x6d95ff1ff4634806, 0x562f21555458339a,
    0xc0ce47f889336346, 0x487823e5089b40d8, 0xe4727c7ebc6d9592, 0x5a8f7277e94970ba,
    0xfca2f406b1c8bb50, 0x5b1f8a95f1791070, 0xd304af9fc9028605, 0x5440ab7fc930e748,
    0x312d25fbca2ab5a1, 0x10f4a4b234a4d575, 0x90301d55047e7473, 0x3b6372886c61591e,
    0x293402b77c444e06, 0x451f34a4d3e97dd7, 0x3158d814d81bc57b, 0x034942425b9bda69,
    0xe2032ff9e532d9bb, 0x62ae066b8b2179e5, 0x9545e10c2f8d71d8, 0x7ff7483eb2d23fc0,
    0x00945fcebdc98d86, 0x8764bbbe99b26ca2, 0x1b1ec62284c0bfc3, 0x58e0fcc4f0aa362b,
    0x5f4abefa878d458d, 0xfd74ac2f9607c519, 0xa4e3fb37df8cbfa9, 0xbf697e43cac574e5,
    0x86f14a3f68f4cd53, 0x24a23d076f1ce522, 0xe725cd8048868cc8, 0xbf3c729eb2464362,
    0xd8f6cd57b3cc1ed8, 0x6329e52425541577, 0x62aa688ad5ae1ac0, 0x0a242566269bf845,
    0x168b1a4753aca74b, 0xf789afefff2e7e3c, 0x6c3362093b6fccdb, 0x4ce8f50bd28c09b2,
    0x006a2db95ae8aa93, 0x975b0d623c3d1a8c, 0x18605d3935338c5b, 0x5bb6f6136cad3c71,
    0x0f53a20701f8d8a6, 0xab8c5ad2e7e93c67, 0x40b5ac5127acaa29, 0x8c7bf63c2075895f,
    0x78bd9f7e014a805c, 0xb2c9e9f4f9c8c032, 0xefd6049827eb91f3, 0x2be459f482c16fbd,
    0xd92ce0c5745aaa8c, 0x0aaa8fb298d965b9, 0x2b37f92c6c803b15, 0x8c54a5e94e0f0e78,
    0x95f9b6e90c0a3032, 0xe7939faa436c7874, 0xd16bfe8f6a8a40c9, 0x44982b86263fd2fa,
    0xe285fb39f984e583, 0x779a8df72d7619d3, 0xf2d79a8de8d5dd1e, 0xd1037354d66684e2,
    0x004c82a4e668a8e5, 0x31d40a7668b044e6, 0xd70578538bd02c11, 0xdb45431078c5f482,
    0x977121bb7f6a51ad, 0x73d5ccbd34eff8dd, 0xe437a07d356e17cd, 0x47b2782043c95627,
    0x9fb251413e41d49a, 0xccd70b60652513d3, 0x1c95b31e8a1b49b2, 0xcae73dfd1bcb4c1b,
    0x34d98331b1f5b70f, 0x784e39f22338d92f, 0x18613d4a064df420, 0xf1d8dae25f0bcebe,
    0x33f77c15ae855efc, 0x3c88b3b912eb109c, 0x956a2ec96bafeea5, 0x1aa005b5e0ad0e87,
    0x5500d70527c4bb8e, 0xe36c57196421cc44, 0x13c4d286cc36ee39, 0x5654a23d818b2a81,
    0x77b1dc13d161abdc, 0x734f44de5f8d5eb5, 0x60717e174a6c89a2, 0xd47d9649266a211e,
    0x5b13a4322bb69e90, 0xf7669609f8b5fc3c, 0x21e6ac55bedcdac9, 0x9b56b62b61166dea,
    0xf48f66b939797e9c, 0x35f332f9c0e6ae9a, 0xcc733f6a9a878db0, 0x3da161e41cc108c2,
    0xb7d74ae535914d51, 0x4d493b0b11d36469, 0xce264d1dfba9741a, 0xa9d1f2dc7436dc06,
    0x70738016604c2a27, 0x231d36e96e93f3d5, 0x7666881197838d19, 0x4a2a83090aaad40c,
    0xf1e761591668b35d, 0x7363236497f730a7, 0x301080e37379dd4d, 0x502dea2971827042,
    0xc2c5eb858f32625f, 0x786afb9edfafbdff, 0xdaee0d868490b2a4, 0x617366b3268609f6,
    0xae0e35a0fe46173e, 0xd1a07de93e824f11, 0x079b8b115ea4cca8, 0x93a99274558faebb,
    0xfb1e6e22e08a03b3, 0xea635fdba3698dd0, 0xcf53659328503a5c, 0xcde3b31e6fd5d780,
    0x8e3e4221d3614413, 0xef14d0d86bf1a22c, 0xe1d830d3f16c5ddb, 0xaabd2b2a451504e1};

const int64_t MIN_LEN = 256 * 1024;
const int64_t AVG_LEN = 1 * 1024 * 1024;
const int64_t MAX_LEN = 2 * 1024 * 1024;

// create a fake null array class with a GetView method returning 0 always
class FakeNullArray {
 public:
  uint8_t GetView(int64_t i) const { return 0; }

  std::shared_ptr<::arrow::DataType> type() const { return ::arrow::null(); }

  int64_t null_count() const { return 0; }
};

static uint64_t GetMask(uint64_t avg_len, uint8_t bit_adjustment) {
  size_t mask_bits = std::log2(avg_len);
  size_t effective_bits = mask_bits + bit_adjustment;
  return ((1ULL << effective_bits) - 1) << (64 - effective_bits);
}

class FastCDC {
 public:
  FastCDC(const LevelInfo& level_info, uint64_t min_len, uint64_t avg_len,
          uint64_t max_len, uint8_t normalization_level = 1)
      : level_info_(level_info),
        min_len_(min_len == 0 ? MIN_LEN : min_len),
        avg_len_(avg_len == 0 ? AVG_LEN : avg_len),
        max_len_(max_len == 0 ? MAX_LEN : max_len),
        mask_s_(GetMask(avg_len_, -normalization_level)),
        mask_l_(GetMask(avg_len_, +normalization_level)) {}

  template <typename T>
  bool Roll(const T value) {
    constexpr size_t BYTE_WIDTH = sizeof(T);
    chunk_size_ += BYTE_WIDTH;
    uint64_t mask;
    if (chunk_size_ < min_len_) {
      return false;
    } else if (chunk_size_ < avg_len_) {
      mask = mask_l_;
    } else {
      mask = mask_s_;
    }
    auto bytes = reinterpret_cast<const uint8_t*>(&value);
    bool match = false;
    for (size_t i = 0; i < BYTE_WIDTH; ++i) {
      hash_ = (hash_ << 1) + GEAR_TABLE[bytes[i]];
      if ((hash_ & mask) == 0) {
        match = true;
      }
    }
    return match;
  }

  bool Roll(std::string_view value) {
    chunk_size_ += value.size();
    uint64_t mask;
    if (chunk_size_ < min_len_) {
      return false;
    } else if (chunk_size_ < avg_len_) {
      mask = mask_l_;
    } else {
      mask = mask_s_;
    }
    bool match = false;
    for (char c : value) {
      hash_ = (hash_ << 1) + GEAR_TABLE[static_cast<uint8_t>(c)];
      if ((hash_ & mask) == 0) {
        match = true;
      }
    }
    return match;
  }

  inline bool Check(bool match) {
    if (match || (chunk_size_ >= max_len_)) {
      chunk_size_ = 0;
      return true;
    } else {
      return false;
    }
  }

  template <typename T>
  const std::vector<std::tuple<int64_t, int64_t, int64_t>> GetBoundaries(
      const int16_t* def_levels, const int16_t* rep_levels, int64_t num_levels,
      const T& leaf_array) {
    std::vector<std::tuple<int64_t, int64_t, int64_t>> result;
    bool has_def_levels = level_info_.def_level > 0;
    bool has_rep_levels = level_info_.rep_level > 0;

    if (!has_rep_levels && !has_def_levels) {
      // fastest path for non-repeated non-null data
      bool val_match;
      int64_t offset = 0;
      int64_t prev_offset = 0;
      while (offset < num_levels) {
        val_match = Roll(leaf_array.GetView(offset));
        ++offset;
        if (Check(val_match)) {
          result.emplace_back(prev_offset, prev_offset, offset - prev_offset);
          prev_offset = offset;
        }
      }
      if (prev_offset < num_levels) {
        result.emplace_back(prev_offset, prev_offset, num_levels - prev_offset);
      }
    } else if (!has_rep_levels) {
      // non-repeated data possibly with nulls
      bool def_match, val_match;
      int64_t offset = 0;
      int64_t prev_offset = 0;
      while (offset < num_levels) {
        def_match = Roll(def_levels[offset]);
        val_match = Roll(leaf_array.GetView(offset));
        ++offset;
        if (Check(def_match || val_match)) {
          result.emplace_back(prev_offset, prev_offset, offset - prev_offset);
          prev_offset = offset;
        }
      }
      if (prev_offset < num_levels) {
        result.emplace_back(prev_offset, prev_offset, num_levels - prev_offset);
      }
    } else {
      // repeated data possibly with nulls
      bool def_match, rep_match, val_match;
      int16_t def_level;
      int16_t rep_level;
      int64_t level_offset = 0;
      int64_t value_offset = 0;
      int64_t record_level_offset = 0;
      int64_t record_value_offset = 0;
      int64_t prev_record_level_offset = 0;
      int64_t prev_record_value_offset = 0;

      while (level_offset < num_levels) {
        def_level = def_levels[level_offset];
        rep_level = rep_levels[level_offset];
        ++level_offset;

        if (rep_level == 0) {
          // record boundary
          record_level_offset = level_offset;
          record_value_offset = value_offset;
        }

        def_match = Roll(def_level);
        rep_match = Roll(rep_level);
        if (def_level >= level_info_.repeated_ancestor_def_level) {
          val_match = Roll(leaf_array.GetView(value_offset));
          ++value_offset;
        } else {
          val_match = false;
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
  const uint64_t min_len_;
  const uint64_t avg_len_;
  const uint64_t max_len_;
  const uint64_t mask_s_;
  const uint64_t mask_l_;
  uint64_t hash_ = 0;
  uint64_t chunk_size_ = 0;
};

}  // namespace internal
}  // namespace parquet
