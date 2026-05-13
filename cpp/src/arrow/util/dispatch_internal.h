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
#include <type_traits>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/cpu_info.h"

namespace arrow::internal {

enum class DispatchLevel : int {
  // These dispatch levels, corresponding to instruction set features,
  // are sorted in increasing order of preference.
  NONE = 0,
  SSE4_2,
  AVX2,
  AVX512,
  NEON,
  SVE128,
  SVE256,
  SVE512,
  MAX
};

/// A dispatch target pairing a dispatch level with a function pointer.
template <typename Func>
struct DynamicDispatchTarget {
  DispatchLevel level = DispatchLevel::NONE;
  Func func = {};
};

template <typename Func>
DynamicDispatchTarget(DispatchLevel, Func) -> DynamicDispatchTarget<Func>;

namespace detail {

/// A trait for checking if a type is a static ``std::array``.
template <typename>
inline constexpr bool is_std_array_v = false;

template <typename T, std::size_t N>
inline constexpr bool is_std_array_v<std::array<T, N>> = true;

}  // namespace detail

/// A concept for an array of functions pointers and their dynamic dispatch level.
template <typename Arr, typename FunctionType>
concept DynamicDispatchTargets =
    detail::is_std_array_v<Arr> &&
    std::is_same_v<typename Arr::value_type, DynamicDispatchTarget<FunctionType>>;

/// Return whether a given dispatch level is static.
///
/// This depends on macros defined in the build options.
constexpr bool DispatchIsStatic(DispatchLevel level) {
  switch (level) {
#ifdef ARROW_HAVE_SSE4_2
    case DispatchLevel::SSE4_2:
#endif
#ifdef ARROW_HAVE_AVX2
    case DispatchLevel::AVX2:
#endif
#ifdef ARROW_HAVE_AVX512
    case DispatchLevel::AVX512:
#endif
#ifdef ARROW_HAVE_NEON
    case DispatchLevel::NEON:
#endif
    case DispatchLevel::NONE:
      return true;
    default:
      return false;
  }
}

/// Return whether all function in the array can be statically dispatched.
template <typename Func>
constexpr bool DispatchFullyStatic(const DynamicDispatchTargets<Func> auto& targets) {
  return std::ranges::all_of(targets, [](const DynamicDispatchTarget<Func>& trgt) {
    return DispatchIsStatic(trgt.level);
  });
}

/// Return whether any function in the array can be statically dispatched.
/// Return false on empty sets.
template <typename Func>
constexpr bool DispatchHasStatic(const DynamicDispatchTargets<Func> auto& targets) {
  return std::ranges::any_of(targets, [](const DynamicDispatchTarget<Func>& trgt) {
    return DispatchIsStatic(trgt.level);
  });
}

/// Find the best dispatch target given a filter.
template <typename Func, typename Filter>
constexpr DynamicDispatchTarget<Func> BestDispatchTarget(
    const DynamicDispatchTargets<Func> auto& targets, Filter filter) {
  DynamicDispatchTarget<Func> best = {};
  for (const auto& trgt : targets) {
    if (trgt.level >= best.level && filter(trgt)) {
      best = trgt;
    }
  }
  return best;
}

/// Find the best dispatch target (no filter).
template <typename Func>
constexpr DynamicDispatchTarget<Func> BestDispatchTarget(
    const DynamicDispatchTargets<Func> auto& targets) {
  return BestDispatchTarget<Func>(targets, [](const auto&) { return true; });
}

#define ARROW_DISPATCH_TARGET_NONE(func)      \
  ::arrow::internal::DynamicDispatchTarget{   \
      ::arrow::internal::DispatchLevel::NONE, \
      (func),                                 \
  },

#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define ARROW_DISPATCH_TARGET_SSE4_2(func)      \
    ::arrow::internal::DynamicDispatchTarget{     \
        ::arrow::internal::DispatchLevel::SSE4_2, \
        (func),                                   \
    },
#else
#  define ARROW_DISPATCH_TARGET_SSE4_2(func)
#endif

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
#  define ARROW_DISPATCH_TARGET_AVX2(func)      \
    ::arrow::internal::DynamicDispatchTarget{   \
        ::arrow::internal::DispatchLevel::AVX2, \
        (func),                                 \
    },
#else
#  define ARROW_DISPATCH_TARGET_AVX2(func)
#endif

#if defined(ARROW_HAVE_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX512)
#  define ARROW_DISPATCH_TARGET_AVX512(func)      \
    ::arrow::internal::DynamicDispatchTarget{     \
        ::arrow::internal::DispatchLevel::AVX512, \
        (func),                                   \
    },
#else
#  define ARROW_DISPATCH_TARGET_AVX512(func)
#endif

#if defined(ARROW_HAVE_NEON)
#  define ARROW_DISPATCH_TARGET_NEON(func)      \
    ::arrow::internal::DynamicDispatchTarget{   \
        ::arrow::internal::DispatchLevel::NEON, \
        (func),                                 \
    },
#else
#  define ARROW_DISPATCH_TARGET_NEON(func)
#endif

#if defined(ARROW_HAVE_SVE128) || defined(ARROW_HAVE_RUNTIME_SVE128)
#  define ARROW_DISPATCH_TARGET_SVE128(func)      \
    ::arrow::internal::DynamicDispatchTarget{     \
        ::arrow::internal::DispatchLevel::SVE128, \
        (func),                                   \
    },
#else
#  define ARROW_DISPATCH_TARGET_SVE128(func)
#endif

#if defined(ARROW_HAVE_SVE256) || defined(ARROW_HAVE_RUNTIME_SVE256)
#  define ARROW_DISPATCH_TARGET_SVE256(func)      \
    ::arrow::internal::DynamicDispatchTarget{     \
        ::arrow::internal::DispatchLevel::SVE256, \
        (func),                                   \
    },
#else
#  define ARROW_DISPATCH_TARGET_SVE256(func)
#endif

#if defined(ARROW_HAVE_SVE512) || defined(ARROW_HAVE_RUNTIME_SVE512)
#  define ARROW_DISPATCH_TARGET_SVE512(func)      \
    ::arrow::internal::DynamicDispatchTarget{     \
        ::arrow::internal::DispatchLevel::SVE512, \
        (func),                                   \
    },
#else
#  define ARROW_DISPATCH_TARGET_SVE512(func)
#endif

/// A concept to specify how dynamic dispatch should be handled.
///
/// A requirement is that the list of available targets must be compile time
/// array with at least one target available for static dispatch.
template <typename T>
concept DynamicDispatchSpec = requires {
  typename T::FunctionType;

  { T::targets() } -> DynamicDispatchTargets<typename T::FunctionType>;
  requires T::targets().size() > 0;
  requires DispatchHasStatic<typename T::FunctionType>(T::targets());
};

/// Refinement of DynamicDispatchSpec where all targets are statically available.
///
/// Subsumes DynamicDispatchSpec, enabling a more specialized DynamicDispatch
/// implementation.
template <typename T>
concept DynamicDispatchFullyStaticSpec =
    DynamicDispatchSpec<T> && DispatchFullyStatic<typename T::FunctionType>(T::targets());

/*
  A facility for dynamic dispatch according to available DispatchLevel.

  Typical use:

    static void my_function_default(...);
    static void my_function_avx2(...);

    struct MyDynamicFunction {
      using FunctionType = decltype(&my_function_default);

      static std::array<DynamicDispatchTarget<FunctionType>, N> targets() {
        return {
          { DispatchLevel::NONE, my_function_default }
    #if defined(ARROW_HAVE_RUNTIME_AVX2)
          , { DispatchLevel::AVX2, my_function_avx2 }
    #endif
        };
      }
    };

    void my_function(...) {
      static DynamicDispatch<MyDynamicFunction> dispatch;
      return dispatch.func(...);
    }
*/

/// Dynamic dispatcher between function with different micro architectures.
///
/// The dispatcher is configured with a ``DynamicDispatchSpec`` to list available
/// targets (function and dispatch level pair).
/// The dispatch mechanism uses a combination of compile time computation and
/// preprocessor macros to fallback to the best static dispatch when, due to build
/// configurations, no tartget is dynamically available.
/// This is for example the case on MacOS where Neon is always available while SVE
/// never is. This is also the case when an Arrow is compiled with and advance baseline.
/// For instance if the baseline is AVX2 and that there is no AVX512 target provided,
/// then the dispatch will be fully static.
///
/// Typical usage involves ``ARROW_DISPATCH_TARGET_<ARCH>`` macros to avoid referencing
/// functions that may not be available on certain build configurations.
///
/// ```cpp
/// struct MyFunctionDyn {
///   using FunctionType = decltype(&MyFuncScalar);
///
///   static constexpr auto targets() {
///     return std::array{
///         ARROW_DISPATCH_TARGET_NONE(&MyFuncScalar)    //
///         ARROW_DISPATCH_TARGET_NEON(&MyFuncNeon)      //
///         ARROW_DISPATCH_TARGET_SSE4_2(&MyFuncSse42)   //
///         ARROW_DISPATCH_TARGET_AVX2(&MyFuncAvx2)      //
///         ARROW_DISPATCH_TARGET_AVX512(&MyFuncAvx512)  //
///     };
///   }
/// };
/// ```
///
/// And then used with the ``DynamicDispatch`` as such:
///
/// ```cpp
/// int MyFunc(const uint8_t* input, int param) {
///     static const DynamicDispatch<MyFuncDyn> dispatch;
///     return dispatch(input, param);
/// }
/// ```
template <DynamicDispatchSpec DynamicFunction>
class DynamicDispatch;

template <DynamicDispatchSpec DynamicFunction>
class DynamicDispatch {
 public:
  using FunctionType = typename DynamicFunction::FunctionType;
  using Target = DynamicDispatchTarget<FunctionType>;
  static constexpr auto kTargets = DynamicFunction::targets();

  DynamicDispatch() {
    const auto best = BestDispatchTarget<FunctionType>(
        kTargets, [this](const Target& trgt) { return IsSupported(trgt.level); });
    func = best.func;
  }

  template <typename... Args>
  auto operator()(Args&&... args) const -> decltype(auto) {
    return func(std::forward<Args>(args)...);
  }

 private:
  FunctionType func = {};

  bool IsSupported(DispatchLevel level) const {
    static const auto cpu_info = arrow::internal::CpuInfo::GetInstance();

    switch (level) {
      case DispatchLevel::NONE:
        return true;
      case DispatchLevel::SSE4_2:
        return cpu_info->IsSupported(CpuInfo::SSE4_2);
      case DispatchLevel::AVX2:
        return cpu_info->IsSupported(CpuInfo::AVX2);
      case DispatchLevel::AVX512:
        return cpu_info->IsSupported(CpuInfo::AVX512);
      case DispatchLevel::NEON:
        return cpu_info->IsSupported(CpuInfo::ASIMD);
      case DispatchLevel::SVE128:
        return cpu_info->IsSupported(CpuInfo::SVE128);
      case DispatchLevel::SVE256:
        return cpu_info->IsSupported(CpuInfo::SVE256);
      case DispatchLevel::SVE512:
        return cpu_info->IsSupported(CpuInfo::SVE512);
      default:
        return false;
    }
  }
};

/// Specialization for the fully-static case: best target is resolved at compile time,
/// no runtime CPU detection needed.
template <DynamicDispatchFullyStaticSpec DynamicFunction>
class DynamicDispatch<DynamicFunction> {
 public:
  using FunctionType = typename DynamicFunction::FunctionType;
  using Target = DynamicDispatchTarget<FunctionType>;
  static constexpr auto kTargets = DynamicFunction::targets();
  static constexpr FunctionType kBest = BestDispatchTarget<FunctionType>(kTargets).func;

  template <typename... Args>
  auto operator()(Args&&... args) const -> decltype(auto) {
    return kBest(std::forward<Args>(args)...);
  }
};

}  // namespace arrow::internal
