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
#include "vendored/safe-math.h"

// Define functions AddWithOverflow, SubtractWithOverflow, MultiplyWithOverflow
// with the signature `bool(T u, T v, T* out)` where T is an integer type.
// On overflow, these functions return true.  Otherwise, false is returned
// and `out` is updated with the result of the operation.

#define OP_WITH_OVERFLOW(_func_name, _psnip_op, _type, _psnip_type) \
  static inline bool _func_name(_type u, _type v, _type* out) {     \
    return !psnip_safe_##_psnip_type##_##_psnip_op(out, u, v);      \
  }

#define OPS_WITH_OVERFLOW(_func_name, _psnip_op)            \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int8_t, int8)     \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int16_t, int16)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int32_t, int32)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int64_t, int64)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint8_t, uint8)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint16_t, uint16) \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint32_t, uint32) \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint64_t, uint64)

OPS_WITH_OVERFLOW(AddWithOverflow, add)
OPS_WITH_OVERFLOW(SubtractWithOverflow, sub)
OPS_WITH_OVERFLOW(MultiplyWithOverflow, mul)
OPS_WITH_OVERFLOW(DivideWithOverflow, div)

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
    ADD_CHECKED,
    SUB, 
    SUB_CHECKED,
};

template <typename T>
using is_unsigned_integer_value = bool_constant<is_integral_v<T> && is_unsigned_v<T>>;

template <typename T>
using is_signed_integer_value = bool_constant<is_integral_v<T> && is_signed_v<T>>;

template <typename T, typename R = T>
using enable_if_signed_integer_t = enable_if_t<is_signed_integer_value<T>::value, R>;

template <typename T, typename R = T>
using enable_if_unsigned_integer_t = enable_if_t<is_unsigned_integer_value<T>::value, R>;

template <typename T, typename R = T>
using enable_if_integer_t = enable_if_t<
    is_signed_integer_value<T>::value || is_unsigned_integer_value<T>::value, R>;

template <typename T, typename R = T>
using enable_if_floating_t = enable_if_t<is_floating_point_v<T>, R>;

struct Add {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_floating_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left + right;
    }

    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_integer_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left + right;
    }
};

struct Sub {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_floating_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left - right;
    }

    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_integer_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left - right;
    }
};

struct AddChecked {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_floating_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left + right;
    }

    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_integer_t<T> Call(Arg0 left, Arg1 right, bool* failure) {
        static_assert(is_same<T, Arg0>::value && is_same<T, Arg1>::value, "");
        T result = 0;
        if (AddWithOverflow(left, right, &result)) {
            *failure = true;
        }
        return result;
    }    
};


struct SubChecked {
    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_floating_t<T> Call(Arg0 left, Arg1 right, bool*) {
        return left - right;
    }

    template <typename T, typename Arg0, typename Arg1>
    static constexpr enable_if_integer_t<T> Call(Arg0 left, Arg1 right, bool* failure) {
        static_assert(is_same<T, Arg0>::value && is_same<T, Arg1>::value, "");
        T result = 0;
        if (SubtractWithOverflow(left, right, &result)) {
            *failure = true;
        }
        return result;
    }    
};

template <typename T, typename Op>
struct arithmetic_op_arr_arr_impl {
    static inline void exec(const void* in_left, const void* in_right, void* out, const int len) {
        const T* left = reinterpret_cast<const T*>(in_left);
        const T* right = reinterpret_cast<const T*>(in_right);
        T* output = reinterpret_cast<T*>(out);

        bool failure = false;
        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<T, T, T>(left[i], right[i], &failure);
        }
    }
};

template <typename T, typename Op>
struct arithmetic_op_arr_scalar_impl {
    static inline void exec(const void* in_left, const void* scalar_right, void* out, const int len) {
        const T* left = reinterpret_cast<const T*>(in_left);
        const T right = *reinterpret_cast<const T*>(scalar_right);
        T* output = reinterpret_cast<T*>(out);

        bool failure = false;
        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<T, T, T>(left[i], right, &failure);
        }
    }
};

template <typename T, typename Op>
struct arithmetic_op_scalar_arr_impl {
    static inline void exec(const void* scalar_left, const void* in_right, void* out, const int len) {
        const T left = *reinterpret_cast<const T*>(scalar_left);
        const T* right = reinterpret_cast<const T*>(in_right);
        T* output = reinterpret_cast<T*>(out);

        bool failure = false;
        for (int i = 0; i < len; ++i) {
            output[i] = Op::template Call<T, T, T>(left, right[i], &failure);
        }
    }
};


template <typename Op, template<typename...> typename Impl>
static inline void arithmetic_op(const int type, const void* in_left, const void* in_right, void* output, const int len) {
    const auto intype = static_cast<arrtype>(type);

    switch (intype) {
    case arrtype::UINT8:
        Impl<uint8_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::INT8:
        Impl<int8_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::UINT16:
        Impl<uint16_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::INT16:
        Impl<int16_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::UINT32:
        Impl<uint32_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::INT32:
        Impl<int32_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::UINT64:
        Impl<uint64_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::INT64:
        Impl<int64_t, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::FLOAT32:
        Impl<float, Op>::exec(in_left, in_right, output, len);
        break;
    case arrtype::FLOAT64:
        Impl<double, Op>::exec(in_left, in_right, output, len);
        break;
    default:
        break;
    }
}

template <template <typename...> class Impl>
static inline void arithmetic_impl(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    const auto opt = static_cast<optype>(op);

    switch (opt) {
    case optype::ADD:
        arithmetic_op<Add, Impl>(type, in_left, in_right, out, len);
        break;
    case optype::ADD_CHECKED:
        arithmetic_op<AddChecked, Impl>(type, in_left, in_right, out, len);
        break;
    case optype::SUB:
        arithmetic_op<Sub, Impl>(type, in_left, in_right, out, len);
        break;
    case optype::SUB_CHECKED:
        arithmetic_op<SubChecked, Impl>(type, in_left, in_right, out, len);
        break;
    default:
        break;
    }
}

extern "C" void FULL_NAME(arithmetic)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_impl<arithmetic_op_arr_arr_impl>(type, op, in_left, in_right, out, len);
}

extern "C" void FULL_NAME(arithmetic_arr_scalar)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_impl<arithmetic_op_arr_scalar_impl>(type, op, in_left, in_right, out, len);
}

extern "C" void FULL_NAME(arithmetic_scalar_arr)(const int type, const int8_t op, const void* in_left, const void* in_right, void* out, const int len) {
    arithmetic_impl<arithmetic_op_scalar_arr_impl>(type, op, in_left, in_right, out, len);    
}