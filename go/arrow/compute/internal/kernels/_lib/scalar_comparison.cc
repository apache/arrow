// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arch.h>
#include <stdint.h>
#include "types.h"

// pack integers into a bitmap in batches of 8
template <int batch_size>
inline void pack_bits(const uint32_t* values, uint8_t* out) {
    for (int i = 0; i < batch_size / 8; ++i) {
        *out++ = (values[0] | values[1]<<1 | values[2]<<2 | values[3]<<3 |
                values[4]<<4 | values[5]<<5 | values[6]<<6 | values[7]<<7);
        values += 8;
    }
}

struct Equal {
    template <typename T>
    static constexpr bool Call(const T& left, const T& right) {
        return left == right;
    }
};

struct NotEqual {
    template <typename T>
    static constexpr bool Call(const T& left, const T& right) {
        return left != right;
    }
};

struct Greater {
    template <typename T>
    static constexpr bool Call(const T& left, const T& right) {
        return left > right;
    }
};

struct GreaterEqual {
    template <typename T>
    static constexpr bool Call(const T& left, const T& right) {
        return left >= right;
    }
};

static inline void set_bit_to(uint8_t* bits, int64_t i, bool bit_is_set) {
    bits[i/8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) & static_cast<uint8_t>(1 << (i % 8));
}

template <typename T, typename Op>
struct compare_primitive_arr_arr {
    static inline void Exec(const void* left_void, const void* right_void, int64_t length, void* out_void, const int offset) {
        const T* left = reinterpret_cast<const T*>(left_void);
        const T* right = reinterpret_cast<const T*>(right_void);
        uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_void);
        static constexpr int kBatchSize = 32;
        int64_t num_batches = length / kBatchSize;
        uint32_t temp_output[kBatchSize];

        if (int prefix = offset % 8) {
            for (int i = prefix; i < 8; ++i) {
                set_bit_to(out_bitmap, i, Op::template Call<T>(*left++, *right++));
            }
            out_bitmap++;
        }

        for (int64_t j = 0; j < num_batches; ++j) {
            for (int i = 0; i < kBatchSize; ++i) {
                temp_output[i] = Op::template Call<T>(*left++, *right++);
            }
            pack_bits<kBatchSize>(temp_output, out_bitmap);
            out_bitmap += kBatchSize / 8;
        }
        int64_t bit_index = 0;
        for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
            set_bit_to(out_bitmap, bit_index++, Op::template Call<T>(*left++, *right++));
        }
    }
};

template <typename T, typename Op>
struct compare_primitive_arr_scalar {
    static inline void Exec(const void* left_void, const void* right_void, int64_t length, void* out_void, const int offset) {
        const T* left = reinterpret_cast<const T*>(left_void);
        const T right = *reinterpret_cast<const T*>(right_void);
        uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_void);
        static constexpr int kBatchSize = 32;
        int64_t num_batches = length / kBatchSize;
        uint32_t temp_output[kBatchSize];

        if (int prefix = offset % 8) {
            for (int i = prefix; i < 8; ++i) {
                set_bit_to(out_bitmap, i, Op::template Call<T>(*left++, right));
            }
            out_bitmap++;
        }

        for (int64_t j = 0; j < num_batches; ++j) {
            for (int i = 0; i < kBatchSize; ++i) {
                temp_output[i] = Op::template Call<T>(*left++, right);
            }
            pack_bits<kBatchSize>(temp_output, out_bitmap);
            out_bitmap += kBatchSize / 8;
        }
        int64_t bit_index = 0;
        for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
            set_bit_to(out_bitmap, bit_index++, Op::template Call<T>(*left++, right));
        }
    }
};

template <typename T, typename Op>
struct compare_primitive_scalar_arr {
    static inline void Exec(const void* left_void, const void* right_void, int64_t length, void* out_void, const int offset) {
        const T left = *reinterpret_cast<const T*>(left_void);
        const T* right = reinterpret_cast<const T*>(right_void);
        uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_void);
        static constexpr int kBatchSize = 32;
        int64_t num_batches = length / kBatchSize;
        uint32_t temp_output[kBatchSize];

        if (int prefix = offset % 8) {
            for (int i = prefix; i < 8; ++i) {
                set_bit_to(out_bitmap, i, Op::template Call<T>(left, *right++));
            }
            out_bitmap++;
        }

        for (int64_t j = 0; j < num_batches; ++j) {
            for (int i = 0; i < kBatchSize; ++i) {
                temp_output[i] = Op::template Call<T>(left, *right++);
            }
            pack_bits<kBatchSize>(temp_output, out_bitmap);
            out_bitmap += kBatchSize / 8;
        }
        int64_t bit_index = 0;
        for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
            set_bit_to(out_bitmap, bit_index++, Op::template Call<T>(left, *right++));
        }
    }
};

enum class cmpop : int8_t {
    EQUAL,
    NOT_EQUAL,
    GREATER,
    GREATER_EQUAL,
    // LESS and LESS_EQUAL are handled by doing flipped
    // versions of GREATER and GREATER_EQUAL
};

template <typename Op, template <typename...> typename Impl>
static inline void comparison_exec(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    const auto ty = static_cast<arrtype>(type);

    switch (ty) {
    case arrtype::UINT8:
        return Impl<uint8_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::INT8:
        return Impl<int8_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::UINT16:
        return Impl<uint16_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::INT16:
        return Impl<int16_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::UINT32:
        return Impl<uint32_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::INT32:
        return Impl<int32_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::UINT64:
        return Impl<uint64_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::INT64:
        return Impl<int64_t, Op>::Exec(left, right, length, out, offset);
    case arrtype::FLOAT32:
        return Impl<float, Op>::Exec(left, right, length, out, offset);
    case arrtype::FLOAT64:
        return Impl<double, Op>::Exec(left, right, length, out, offset);
    default:
        break;
    }
}

extern "C" void FULL_NAME(comparison_equal_arr_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Equal, compare_primitive_arr_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_equal_arr_scalar)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Equal, compare_primitive_arr_scalar>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_equal_scalar_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Equal, compare_primitive_scalar_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_not_equal_arr_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<NotEqual, compare_primitive_arr_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_not_equal_arr_scalar)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<NotEqual, compare_primitive_arr_scalar>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_not_equal_scalar_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<NotEqual, compare_primitive_scalar_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_arr_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Greater, compare_primitive_arr_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_arr_scalar)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Greater, compare_primitive_arr_scalar>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_scalar_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<Greater, compare_primitive_scalar_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_equal_arr_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<GreaterEqual, compare_primitive_arr_arr>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_equal_arr_scalar)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<GreaterEqual, compare_primitive_arr_scalar>(type, left, right, out, length, offset);
}

extern "C" void FULL_NAME(comparison_greater_equal_scalar_arr)(const int type, const void* left, const void* right, void* out, const int64_t length, const int offset) {
    comparison_exec<GreaterEqual, compare_primitive_scalar_arr>(type, left, right, out, length, offset);
}
