// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <limits>

#include "R_ext/Arith.h"

#undef FALSE
#undef TRUE
#undef NA_LOGICAL

extern "C" {
typedef enum {
  FALSE = 0,
  TRUE = 1,
  NA_LOGICAL = std::numeric_limits<int>::min()
} Rboolean;
}

#define R_EXT_BOOLEAN_H_

#define R_NO_REMAP
#define STRICT_R_HEADERS
#include "Rinternals.h"
#undef STRICT_R_HEADERS
#undef R_NO_REMAP

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wattributes"
#endif

#ifdef __GNUC__
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wattributes"
#endif
// clang-format on

#include "cpp11/altrep.hpp"

namespace cpp11 {
namespace literals {

constexpr R_xlen_t operator"" _xl(unsigned long long int value) { return value; }

}  // namespace literals
}  // namespace cpp11
