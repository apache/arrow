// Copyright (C) 2022 Joaquin M Lopez Munoz.
// Copyright (C) 2022 Christian Mazakas
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_UNORDERED_DETAIL_PRIME_FMOD_HPP
#define BOOST_UNORDERED_DETAIL_PRIME_FMOD_HPP

#include <boost/cstdint.hpp>
#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/size.hpp>

#include <boost/config.hpp>

#include <climits>
#include <cstddef>

#if defined(SIZE_MAX)
#if ((((SIZE_MAX >> 16) >> 16) >> 16) >> 15) != 0
#define BOOST_UNORDERED_FCA_HAS_64B_SIZE_T
#endif
#elif defined(UINTPTR_MAX) /* used as proxy for std::size_t */
#if ((((UINTPTR_MAX >> 16) >> 16) >> 16) >> 15) != 0
#define BOOST_UNORDERED_FCA_HAS_64B_SIZE_T
#endif
#endif

#if defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T) && defined(_MSC_VER)
#include <intrin.h>
#endif

namespace boost {
  namespace unordered {
    namespace detail {
      template <class = void> struct prime_fmod_size
      {
        // Because we compile for C++03, we don't have access to any inline
        // initialization for array data members so the definitions must exist
        // out-of-line. To keep the library header-only, we introduce a dummy
        // template parameter which permits the definition to be included in
        // multiple TUs without conflict.
        //
        static std::size_t sizes[];
        static std::size_t const sizes_len;
        static std::size_t (*positions[])(std::size_t);

#if defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)
        static boost::uint64_t inv_sizes32[];
        static std::size_t const inv_sizes32_len;
#endif /* defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T) */

        static inline std::size_t size_index(std::size_t n)
        {
          std::size_t i = 0;
          for (; i < (sizes_len - 1); ++i) {
            if (sizes[i] >= n) {
              break;
            }
          }
          return i;
        }

        static inline std::size_t size(std::size_t size_index)
        {
          return sizes[size_index];
        }

        template <std::size_t Size> static std::size_t modulo(std::size_t hash)
        {
          return hash % Size;
        }

#if defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)
        // We emulate the techniques taken from:
        // Faster Remainder by Direct Computation: Applications to Compilers and
        // Software Libraries
        // https://arxiv.org/abs/1902.01961
        //
        // In essence, use fancy math to directly calculate the remainder (aka
        // modulo) exploiting how compilers transform division
        //

        static inline boost::uint64_t get_remainder(
          boost::uint64_t fractional, boost::uint32_t d)
        {
#if defined(_MSC_VER)
          // use MSVC intrinsics when available to avoid promotion to 128 bits

          return __umulh(fractional, d);
#elif defined(BOOST_HAS_INT128)
          return static_cast<boost::uint64_t>(
            ((boost::uint128_type)fractional * d) >> 64);
#else
          // portable implementation in the absence of boost::uint128_type on 64
          // bits, which happens at least in GCC 4.5 and prior

          boost::uint64_t r1 = (fractional & UINT32_MAX) * d;
          boost::uint64_t r2 = (fractional >> 32) * d;
          r2 += r1 >> 32;
          return r2 >> 32;
#endif /* defined(_MSC_VER) */
        }

        static inline boost::uint32_t fast_modulo(
          boost::uint32_t a, boost::uint64_t M, boost::uint32_t d)
        {
          boost::uint64_t fractional = M * a;
          return (boost::uint32_t)(get_remainder(fractional, d));
        }
#endif /* defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T) */

        static inline std::size_t position(
          std::size_t hash, std::size_t size_index)
        {
#if defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)
          std::size_t sizes_under_32bit = inv_sizes32_len;
          if (BOOST_LIKELY(size_index < sizes_under_32bit)) {
#if defined(__MSVC_RUNTIME_CHECKS)
            return fast_modulo(
              boost::uint32_t(hash & 0xffffffffu) + boost::uint32_t(hash >> 32),
              inv_sizes32[size_index], boost::uint32_t(sizes[size_index]));
#else
            return fast_modulo(
              boost::uint32_t(hash) + boost::uint32_t(hash >> 32),
              inv_sizes32[size_index], boost::uint32_t(sizes[size_index]));
#endif
          } else {
            return positions[size_index - sizes_under_32bit](hash);
          }
#else
          return positions[size_index](hash);
#endif /* defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T) */
        }
      }; // prime_fmod_size

#define BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT_INCOMPLETE                      \
  (13ul)(29ul)(53ul)(97ul)(193ul)(389ul)(769ul)(1543ul)(3079ul)(6151ul)(       \
    12289ul)(24593ul)(49157ul)(98317ul)(196613ul)(393241ul)(786433ul)(         \
    1572869ul)(3145739ul)(6291469ul)(12582917ul)(25165843ul)(50331653ul)(      \
    100663319ul)(201326611ul)(402653189ul)(805306457ul)(1610612741ul)(         \
    3221225473ul)

#if !defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)

#define BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT                                 \
  BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT_INCOMPLETE(4294967291ul)

#define BOOST_UNORDERED_PRIME_FMOD_SIZES_64BIT

#else

#define BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT                                 \
  BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT_INCOMPLETE

// The original sequence here is this:
// (6442450939ul)
// (12884901893ul)
// (25769803751ul)
// (51539607551ul)
// (103079215111ul)
// (206158430209ul)
// (412316860441ul)
// (824633720831ul)
// (1649267441651ul)
//
// but this causes problems on versions of mingw where the `long` type is 32
// bits, even for 64-bit targets. We work around this by replacing the literals
// with compile-time arithmetic, using bitshifts to reconstruct the number.
//

// clang-format off
#define BOOST_UNORDERED_PRIME_FMOD_SIZES_64BIT                                   \
  ((boost::ulong_long_type(1ul) << 32)   + boost::ulong_long_type(2147483643ul)) \
  ((boost::ulong_long_type(3ul) << 32)   + boost::ulong_long_type(5ul))          \
  ((boost::ulong_long_type(5ul) << 32)   + boost::ulong_long_type(4294967271ul)) \
  ((boost::ulong_long_type(11ul) << 32)  + boost::ulong_long_type(4294967295ul)) \
  ((boost::ulong_long_type(24ul) << 32)  + boost::ulong_long_type(7ul))          \
  ((boost::ulong_long_type(48ul) << 32)  + boost::ulong_long_type(1ul))          \
  ((boost::ulong_long_type(96ul) << 32)  + boost::ulong_long_type(25ul))         \
  ((boost::ulong_long_type(191ul) << 32) + boost::ulong_long_type(4294967295ul)) \
  ((boost::ulong_long_type(383ul) << 32) + boost::ulong_long_type(4294967283ul))
      // clang-format on

#endif /* BOOST_UNORDERED_FCA_HAS_64B_SIZE_T */

#define BOOST_UNORDERED_PRIME_FMOD_SIZES                                       \
  BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT BOOST_UNORDERED_PRIME_FMOD_SIZES_64BIT

      template <class T>
      std::size_t prime_fmod_size<T>::sizes[] = {
        BOOST_PP_SEQ_ENUM(BOOST_UNORDERED_PRIME_FMOD_SIZES)};

      template <class T>
      std::size_t const prime_fmod_size<T>::sizes_len = BOOST_PP_SEQ_SIZE(
        BOOST_UNORDERED_PRIME_FMOD_SIZES);

// Similarly here, we have to re-express the integer initialization using
// arithmetic such that each literal can fit in a 32-bit value.
//
#if defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)
      // clang-format off
        template <class T>
        boost::uint64_t prime_fmod_size<T>::inv_sizes32[] = {
          (boost::ulong_long_type(330382099ul) << 32) + boost::ulong_long_type(2973438898ul) /* = 1418980313362273202 */,
          (boost::ulong_long_type(148102320ul) << 32) + boost::ulong_long_type(2369637129ul) /* = 636094623231363849 */,
          (boost::ulong_long_type(81037118ul) << 32)  + boost::ulong_long_type(3403558990ul) /* = 348051774975651918 */,
          (boost::ulong_long_type(44278013ul) << 32)  + boost::ulong_long_type(1549730468ul) /* = 190172619316593316 */,
          (boost::ulong_long_type(22253716ul) << 32)  + boost::ulong_long_type(2403401389ul) /* = 95578984837873325 */,
          (boost::ulong_long_type(11041047ul) << 32)  + boost::ulong_long_type(143533612ul)  /* = 47420935922132524 */,
          (boost::ulong_long_type(5585133ul) << 32)   + boost::ulong_long_type(106117528ul)  /* = 23987963684927896 */,
          (boost::ulong_long_type(2783517ul) << 32)   + boost::ulong_long_type(1572687312ul) /* = 11955116055547344 */,
          (boost::ulong_long_type(1394922ul) << 32)   + boost::ulong_long_type(3428720239ul) /* = 5991147799191151 */,
          (boost::ulong_long_type(698255ul) << 32)    + boost::ulong_long_type(552319807ul)  /* = 2998982941588287 */,
          (boost::ulong_long_type(349496ul) << 32)    + boost::ulong_long_type(3827689953ul) /* = 1501077717772769 */,
          (boost::ulong_long_type(174641ul) << 32)    + boost::ulong_long_type(3699438549ul) /* = 750081082979285 */,
          (boost::ulong_long_type(87372ul) << 32)     + boost::ulong_long_type(1912757574ul) /* = 375261795343686 */,
          (boost::ulong_long_type(43684ul) << 32)     + boost::ulong_long_type(3821029929ul) /* = 187625172388393 */,
          (boost::ulong_long_type(21844ul) << 32)     + boost::ulong_long_type(3340590800ul) /* = 93822606204624 */,
          (boost::ulong_long_type(10921ul) << 32)     + boost::ulong_long_type(4175852267ul) /* = 46909513691883 */,
          (boost::ulong_long_type(5461ul) << 32)      + boost::ulong_long_type(1401829642ul) /* = 23456218233098 */,
          (boost::ulong_long_type(2730ul) << 32)      + boost::ulong_long_type(2826028947ul) /* = 11728086747027 */,
          (boost::ulong_long_type(1365ul) << 32)      + boost::ulong_long_type(1411150351ul) /* = 5864041509391 */,
          (boost::ulong_long_type(682ul) << 32)       + boost::ulong_long_type(2857253105ul) /* = 2932024948977 */,
          (boost::ulong_long_type(341ul) << 32)       + boost::ulong_long_type(1431073224ul) /* = 1466014921160 */,
          (boost::ulong_long_type(170ul) << 32)       + boost::ulong_long_type(2862758116ul) /* = 733007198436 */,
          (boost::ulong_long_type(85ul) << 32)        + boost::ulong_long_type(1431619357ul) /* = 366503839517 */,
          (boost::ulong_long_type(42ul) << 32)        + boost::ulong_long_type(2863269661ul) /* = 183251896093 */,
          (boost::ulong_long_type(21ul) << 32)        + boost::ulong_long_type(1431647119ul) /* = 91625960335 */,
          (boost::ulong_long_type(10ul) << 32)        + boost::ulong_long_type(2863310962ul) /* = 45812983922 */,
          (boost::ulong_long_type(5ul) << 32)         + boost::ulong_long_type(1431653234ul) /* = 22906489714 */,
          (boost::ulong_long_type(2ul) << 32)         + boost::ulong_long_type(2863311496ul) /* = 11453246088 */,
          (boost::ulong_long_type(1ul) << 32)         + boost::ulong_long_type(1431655764ul) /* = 5726623060 */,
        };
      // clang-format on

      template <class T>
      std::size_t const
        prime_fmod_size<T>::inv_sizes32_len = sizeof(inv_sizes32) /
                                              sizeof(inv_sizes32[0]);

#endif /* defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T) */

#define BOOST_UNORDERED_PRIME_FMOD_POSITIONS_ELEMENT(z, _, n)                  \
  prime_fmod_size<T>::template modulo<n>,

      template <class T>
      std::size_t (*prime_fmod_size<T>::positions[])(std::size_t) = {
#if !defined(BOOST_UNORDERED_FCA_HAS_64B_SIZE_T)
        BOOST_PP_SEQ_FOR_EACH(BOOST_UNORDERED_PRIME_FMOD_POSITIONS_ELEMENT, ~,
          BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT)
#else
        BOOST_PP_SEQ_FOR_EACH(BOOST_UNORDERED_PRIME_FMOD_POSITIONS_ELEMENT, ~,
          BOOST_UNORDERED_PRIME_FMOD_SIZES_64BIT)
#endif
      };

#undef BOOST_UNORDERED_PRIME_FMOD_POSITIONS_ELEMENT
#undef BOOST_UNORDERED_PRIME_FMOD_SIZES
#undef BOOST_UNORDERED_PRIME_FMOD_SIZES_64BIT
#undef BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT
#undef BOOST_UNORDERED_PRIME_FMOD_SIZES_32BIT_INCOMPLETE
    } // namespace detail
  }   // namespace unordered
} // namespace boost

#endif // BOOST_UNORDERED_DETAIL_PRIME_FMOD_HPP
