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
#include <math.h>
#include <stdint.h>
#include <limits.h>
#include "types.h"
#include "vendored/safe-math.h"

// Corresponds to equivalent ArithmeticOp enum in base_arithmetic.go
// for passing across which operation to perform. This allows simpler
// implementation at the cost of having to pass the extra int8 and
// perform a switch.
//
// In cases of small arrays, this is completely negligible. In cases
// of large arrays, the time saved by using SIMD here is significantly
// worth the cost.
enum class optype : int8_t {
    ADD,
    SUB,
    MUL,
    DIV,
    ABSOLUTE_VALUE,
    NEGATE,
    SQRT,
    POWER,
    SIN,
    COS,
    TAN,
    ASIN,
    ACOS,
    ATAN,
    ATAN2,
    LN,
    LOG10,
    LOG2,
    LOG1P,
    LOGB,
    SIGN,

    // this impl doesn't actually perform any overflow checks as we need
    // to only run overflow checks on non-null entries
    ADD_CHECKED,
    SUB_CHECKED,
    MUL_CHECKED,
    DIV_CHECKED,
    ABSOLUTE_VALUE_CHECKED,
    NEGATE_CHECKED,
    SQRT_CHECKED,
    POWER_CHECKED,
    SIN_CHECKED,
    COS_CHECKED,
    TAN_CHECKED,
    ASIN_CHECKED,
    ACOS_CHECKED,    
    LN_CHECKED,
    LOG10_CHECKED,
    LOG2_CHECKED,
    LOG1P_CHECKED,
    LOGB_CHECKED,
};

struct Add {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        if constexpr (is_arithmetic_v<T>)
            return left + right;
    }
};

struct Sub {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        if constexpr (is_arithmetic_v<T>)
            return left - right;
    }
};

struct AddChecked {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        static_assert(is_same<T, Arg0>::value && is_same<T, Arg1>::value, "");
        if constexpr(is_arithmetic_v<T>) {
            return left + right;
        }
    }
};


struct SubChecked {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        static_assert(is_same<T, Arg0>::value && is_same<T, Arg1>::value, "");
        if constexpr(is_arithmetic_v<T>) {
            return left - right;
        }
    }
};

template <typename T>
using maybe_make_unsigned = conditional_t<is_integral_v<T> && !is_same_v<T, bool>, make_unsigned_t<T>, T>;

template <typename T, typename Unsigned = maybe_make_unsigned<T>>
constexpr Unsigned to_unsigned(T signed_) {
    return static_cast<Unsigned>(signed_);
}

struct Multiply {
    static_assert(is_same_v<decltype(int8_t() * int8_t()), int32_t>, "");
    static_assert(is_same_v<decltype(uint8_t() * uint8_t()), int32_t>, "");
    static_assert(is_same_v<decltype(int16_t() * int16_t()), int32_t>, "");
    static_assert(is_same_v<decltype(uint16_t() * uint16_t()), int32_t>, "");
    static_assert(is_same_v<decltype(int32_t() * int32_t()), int32_t>, "");
    static_assert(is_same_v<decltype(uint32_t() * uint32_t()), uint32_t>, "");
    static_assert(is_same_v<decltype(int64_t() * int64_t()), int64_t>, "");
    static_assert(is_same_v<decltype(uint64_t() * uint64_t()), uint64_t>, "");

    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        static_assert(is_same_v<T, Arg0> && is_same_v<T, Arg1>, "");
        if constexpr(is_floating_point_v<T>) {
            return left * right;
        } else if constexpr(is_unsigned_v<T> && !is_same_v<T, uint16_t>) {
            return left * right;
        } else if constexpr(is_signed_v<T> && !is_same_v<T, int16_t>) {
            return to_unsigned(left) * to_unsigned(right);
        } else if constexpr(is_same_v<T, int16_t> || is_same_v<T, uint16_t>) {
            // multiplication of 16 bit integer types implicitly promotes to
            // signed 32 bit integer. However, some inputs may overflow (which
            // triggers undefined behavior). Therefore we first cast to 32 bit
            // unsigned integers where overflow is well defined.
            return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
        }
    }
};

struct MultiplyChecked {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr T Call(Arg0 left, Arg1 right) {
        static_assert(is_same_v<T, Arg0> && is_same_v<T, Arg1>, "");
        if constexpr(is_arithmetic_v<T>) {
            return left * right;
        }
    }
};

struct AbsoluteValue {
    template <typename T, typename Arg>
    static constexpr T Call(Arg input) {
        if constexpr(is_same_v<Arg, float>) {
            *(((int*)&input)+0) &= 0x7fffffff;
            return input;
        } else if constexpr(is_same_v<Arg, double>) {
            *(((int*)&input)+1) &= 0x7fffffff;
            return input;
        } else if constexpr(is_unsigned_v<Arg>) {
            return input;
        } else {
            const auto mask = input >> (sizeof(Arg) * CHAR_BIT - 1);
            return (input + mask) ^ mask;
        }
    }
};

struct AbsoluteValueChecked {
    template <typename T, typename Arg>
    static constexpr T Call(Arg input) {
        if constexpr(is_same_v<Arg, float>) {
            *(((int*)&input)+0) &= 0x7fffffff;
            return input;
        } else if constexpr(is_same_v<Arg, double>) {
            *(((int*)&input)+1) &= 0x7fffffff;
            return input;
        } else if constexpr(is_unsigned_v<Arg>) {
            return input;
        } else {
            const auto mask = input >> (sizeof(Arg) * CHAR_BIT - 1);
            return (input + mask) ^ mask;
        }
    }
};

struct Negate {
    template <typename T, typename Arg>
    static constexpr T Call(Arg input) {
        if constexpr(is_floating_point_v<Arg>) {
            return -input;
        } else if constexpr(is_unsigned_v<Arg>) {
            return ~input + 1;
        } else {
            return -input;
        }
    }
};

struct NegateChecked {
    template <typename T, typename Arg>
    static constexpr T Call(Arg input) {
        static_assert(is_same_v<T, Arg>, "");
        if constexpr(is_floating_point_v<Arg>) {
            return -input;
        } else if constexpr(is_unsigned_v<Arg>) {
            return 0;
        } else {
            return -input;
        }
    }
};

struct Sign {
    template <typename T, typename Arg>
    static constexpr T Call(Arg input) {
        if constexpr(is_floating_point_v<Arg>) {
            return isnan(input) ? input : ((input == 0) ? 0 : (signbit(input) ? -1 : 1));
        } else if constexpr(is_unsigned_v<Arg>) {
            return input > 0 ? 1 : 0;
        } else if constexpr(is_signed_v<Arg>) {
            return input > 0 ? 1 : (input ? -1 : 0);
        }
    }
};

template <typename T, typename Op, typename OutT = T>
struct arithmetic_op_arr_arr_impl {
    static inline void exec(const void* in_left, const void* in_right, void* out, const int len) {
        const T* left = reinterpret_cast<const T*>(in_left);
        const T* right = reinterpret_cast<const T*>(in_right);
        OutT* output = reinterpret_cast<OutT*>(out);

        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<OutT, T, T>(left[i], right[i]);
        }
    }
};

template <typename T, typename Op, typename OutT = T>
struct arithmetic_op_arr_scalar_impl {
    static inline void exec(const void* in_left, const void* scalar_right, void* out, const int len) {
        const T* left = reinterpret_cast<const T*>(in_left);
        const T right = *reinterpret_cast<const T*>(scalar_right);
        OutT* output = reinterpret_cast<OutT*>(out);

        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<OutT, T, T>(left[i], right);
        }
    }
};

template <typename T, typename Op, typename OutT = T>
struct arithmetic_op_scalar_arr_impl {
    static inline void exec(const void* scalar_left, const void* in_right, void* out, const int len) {
        const T left = *reinterpret_cast<const T*>(scalar_left);
        const T* right = reinterpret_cast<const T*>(in_right);
        OutT* output = reinterpret_cast<OutT*>(out);

        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<OutT, T, T>(left, right[i]);
        }
    }
};

template <typename T, typename Op, typename OutT = T>
struct arithmetic_unary_op_impl {
    static inline void exec(const void* arg, void* out, const int len) {
        const T* input = reinterpret_cast<const T*>(arg);
        OutT* output = reinterpret_cast<OutT*>(out);

        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<OutT, T>(input[i]);
        }
    }
};

template <typename Op, template<typename...> typename Impl>
static inline void arithmetic_op(const int type, const void* in_left, const void* in_right, void* output, const int len) {
    const auto intype = static_cast<arrtype>(type);

    switch (intype) {
    case arrtype::UINT8:
        return Impl<uint8_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::INT8:
        return Impl<int8_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::UINT16:
        return Impl<uint16_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::INT16:
        return Impl<int16_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::UINT32:
        return Impl<uint32_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::INT32:
        return Impl<int32_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::UINT64:
        return Impl<uint64_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::INT64:
        return Impl<int64_t, Op>::exec(in_left, in_right, output, len);
    case arrtype::FLOAT32:
        return Impl<float, Op>::exec(in_left, in_right, output, len);
    case arrtype::FLOAT64:
        return Impl<double, Op>::exec(in_left, in_right, output, len);
    default:
        break;
    }
}

template <typename Op, template <typename...> typename Impl, typename Input>
static inline void arithmetic_op(const int otype, const void* input, void* output, const int len) {
    const auto outtype = static_cast<arrtype>(otype);

    switch (outtype) {
    case arrtype::UINT8:
        return Impl<Input, Op, uint8_t>::exec(input, output, len);
    case arrtype::INT8:
        return Impl<Input, Op, int8_t>::exec(input, output, len);
    case arrtype::UINT16:
        return Impl<Input, Op, uint16_t>::exec(input, output, len);
    case arrtype::INT16:
        return Impl<Input, Op, int16_t>::exec(input, output, len);
    case arrtype::UINT32:
        return Impl<Input, Op, uint32_t>::exec(input, output, len);
    case arrtype::INT32:
        return Impl<Input, Op, int32_t>::exec(input, output, len);
    case arrtype::UINT64:
        return Impl<Input, Op, uint64_t>::exec(input, output, len);
    case arrtype::INT64:
        return Impl<Input, Op, int64_t>::exec(input, output, len);
    case arrtype::FLOAT32:
        return Impl<Input, Op, float>::exec(input, output, len);
    case arrtype::FLOAT64:
        return Impl<Input, Op, double>::exec(input, output, len);
    default:
        break;
    }
}


template <typename Op, template <typename...> typename Impl>
static inline void arithmetic_op(const int type, const void* input, void* output, const int len) {
    const auto intype = static_cast<arrtype>(type);

    switch (intype) {
    case arrtype::UINT8:
        return Impl<uint8_t, Op>::exec(input, output, len);
    case arrtype::INT8:
        return Impl<int8_t, Op>::exec(input, output, len);
    case arrtype::UINT16:
        return Impl<uint16_t, Op>::exec(input, output, len);
    case arrtype::INT16:
        return Impl<int16_t, Op>::exec(input, output, len);
    case arrtype::UINT32:
        return Impl<uint32_t, Op>::exec(input, output, len);
    case arrtype::INT32:
        return Impl<int32_t, Op>::exec(input, output, len);
    case arrtype::UINT64:
        return Impl<uint64_t, Op>::exec(input, output, len);
    case arrtype::INT64:
        return Impl<int64_t, Op>::exec(input, output, len);
    case arrtype::FLOAT32:
        return Impl<float, Op>::exec(input, output, len);
    case arrtype::FLOAT64:
        return Impl<double, Op>::exec(input, output, len);
    default:
        break;
    }
}

template <typename Op, template <typename...> typename Impl>
static inline void arithmetic_op(const int itype, const int otype, const void* input, void* output, const int len) {
    const auto intype = static_cast<arrtype>(itype);

    switch (intype) {
    case arrtype::UINT8:
        return arithmetic_op<Op, Impl, uint8_t>(otype, input, output, len);
    case arrtype::INT8:
        return arithmetic_op<Op, Impl, int8_t>(otype, input, output, len);
    case arrtype::UINT16:
        return arithmetic_op<Op, Impl, uint16_t>(otype, input, output, len);
    case arrtype::INT16:
        return arithmetic_op<Op, Impl, int16_t>(otype, input, output, len);
    case arrtype::UINT32:
        return arithmetic_op<Op, Impl, uint32_t>(otype, input, output, len);
    case arrtype::INT32:
        return arithmetic_op<Op, Impl, int32_t>(otype, input, output, len);
    case arrtype::UINT64:
        return arithmetic_op<Op, Impl, uint64_t>(otype, input, output, len);
    case arrtype::INT64:
        return arithmetic_op<Op, Impl, int64_t>(otype, input, output, len);
    case arrtype::FLOAT32:
        return arithmetic_op<Op, Impl, float>(otype, input, output, len);
    case arrtype::FLOAT64:
        return arithmetic_op<Op, Impl, double>(otype, input, output, len);
    default:
        break;
    }
}

template <template <typename...> class Impl>
static inline void arithmetic_unary_impl_same_types(const int type, const int8_t op, const void* input, void* output, const int len) {
    const auto opt = static_cast<optype>(op);

    switch (opt) {
    case optype::ABSOLUTE_VALUE:
        return arithmetic_op<AbsoluteValue, Impl>(type, input, output, len);
    case optype::ABSOLUTE_VALUE_CHECKED:
        return arithmetic_op<AbsoluteValueChecked, Impl>(type, input, output, len);
    case optype::NEGATE:
        return arithmetic_op<Negate, Impl>(type, input, output, len);
    case optype::NEGATE_CHECKED:
        return arithmetic_op<NegateChecked, Impl>(type, input, output, len);
    case optype::SIGN:
        return arithmetic_op<Sign, Impl>(type, input, output, len);
    default:
        break;
    }
}


template <template <typename...> class Impl>
static inline void arithmetic_unary_impl(const int itype, const int otype, const int8_t op, const void* input, void* output, const int len) {
    const auto opt = static_cast<optype>(op);

    switch (opt) {
    case optype::SIGN:
        return arithmetic_op<Sign, Impl>(itype, otype, input, output, len);
    default:
        break;
    }
}

template <template <typename...> class Impl>
static inline void arithmetic_binary_impl(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    const auto opt = static_cast<optype>(op);

    switch (opt) {
    case optype::ADD:
        return arithmetic_op<Add, Impl>(type, in_left, in_right, out, len);
    case optype::ADD_CHECKED:
        return arithmetic_op<AddChecked, Impl>(type, in_left, in_right, out, len);
    case optype::SUB:
        return arithmetic_op<Sub, Impl>(type, in_left, in_right, out, len);
    case optype::SUB_CHECKED:
        return arithmetic_op<SubChecked, Impl>(type, in_left, in_right, out, len);
    case optype::MUL:
        return arithmetic_op<Multiply, Impl>(type, in_left, in_right, out, len);
    case optype::MUL_CHECKED:
        return arithmetic_op<MultiplyChecked, Impl>(type, in_left, in_right, out, len);
    default:
        // don't implement divide here as we can only divide on non-null entries
        // so we can avoid dividing by zero
        break;
    }
}

extern "C" void FULL_NAME(arithmetic_binary)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_binary_impl<arithmetic_op_arr_arr_impl>(type, op, in_left, in_right, out, len);
}

extern "C" void FULL_NAME(arithmetic_arr_scalar)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_binary_impl<arithmetic_op_arr_scalar_impl>(type, op, in_left, in_right, out, len);
}

extern "C" void FULL_NAME(arithmetic_scalar_arr)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_binary_impl<arithmetic_op_scalar_arr_impl>(type, op, in_left, in_right, out, len);
}

extern "C" void FULL_NAME(arithmetic_unary_same_types)(const int type, const int8_t op, const void* input, void* output, const int len) {
    arithmetic_unary_impl_same_types<arithmetic_unary_op_impl>(type, op, input, output, len);
}

extern "C" void FULL_NAME(arithmetic_unary_diff_type)(const int itype, const int otype, const int8_t op, const void* input, void* output, const int len) {
    arithmetic_unary_impl<arithmetic_unary_op_impl>(itype, otype, op, input, output, len);
}
