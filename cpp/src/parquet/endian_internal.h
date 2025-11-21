#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

#include "arrow/util/endian.h"

#include "parquet/types.h"

namespace parquet {
namespace internal {

inline uint16_t bswap16(uint16_t x) { return static_cast<uint16_t>((x << 8) | (x >> 8)); }
inline uint32_t bswap32(uint32_t x) {
  return ((x & 0x000000FFu) << 24) | ((x & 0x0000FF00u) << 8) |
         ((x & 0x00FF0000u) >> 8) | ((x & 0xFF000000u) >> 24);
}
inline uint64_t bswap64(uint64_t x) {
  return ((x & 0x00000000000000FFull) << 56) |
         ((x & 0x000000000000FF00ull) << 40) |
         ((x & 0x0000000000FF0000ull) << 24) |
         ((x & 0x00000000FF000000ull) << 8) |
         ((x & 0x000000FF00000000ull) >> 8) |
         ((x & 0x0000FF0000000000ull) >> 24) |
         ((x & 0x00FF000000000000ull) >> 40) |
         ((x & 0xFF00000000000000ull) >> 56);
}

template <typename T, typename Enable = void>
struct ByteSwap;

template <typename T>
struct ByteSwap< T, typename std::enable_if<sizeof(T) == 1 && std::is_integral<T>::value>::type > {
  static inline T Do(T v) { return v; }
};

template <typename T>
struct ByteSwap< T, typename std::enable_if<sizeof(T) == 2 && std::is_integral<T>::value>::type > {
  static inline T Do(T v) {
    uint16_t u;
    std::memcpy(&u, &v, sizeof(u));
    u = bswap16(u);
    std::memcpy(&v, &u, sizeof(v));
    return v;
  }
};

template <typename T>
struct ByteSwap< T, typename std::enable_if<sizeof(T) == 4 && std::is_integral<T>::value>::type > {
  static inline T Do(T v) {
    uint32_t u;
    std::memcpy(&u, &v, sizeof(u));
    u = bswap32(u);
    std::memcpy(&v, &u, sizeof(v));
    return v;
  }
};

template <typename T>
struct ByteSwap< T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type > {
  static inline T Do(T v) {
    uint64_t u;
    std::memcpy(&u, &v, sizeof(u));
    u = bswap64(u);
    std::memcpy(&v, &u, sizeof(v));
    return v;
  }
};

template <typename T>
struct ByteSwap< T, typename std::enable_if<std::is_floating_point<T>::value && sizeof(T) == 4>::type > {
  static inline T Do(T v) {
    uint32_t u;
    std::memcpy(&u, &v, sizeof(u));
    u = bswap32(u);
    std::memcpy(&v, &u, sizeof(v));
    return v;
  }
};

template <typename T>
struct ByteSwap< T, typename std::enable_if<std::is_floating_point<T>::value && sizeof(T) == 8>::type > {
  static inline T Do(T v) {
    uint64_t u;
    std::memcpy(&u, &v, sizeof(u));
    u = bswap64(u);
    std::memcpy(&v, &u, sizeof(v));
    return v;
  }
};

template <>
struct ByteSwap<Int96, void> {
  static inline Int96 Do(Int96 v) {
    v.value[0] = bswap32(v.value[0]);
    v.value[1] = bswap32(v.value[1]);
    v.value[2] = bswap32(v.value[2]);
    return v;
  }
};

#if defined(ARROW_ENSURE_S390X_ENDIANNESS)
#if defined(__s390__) || defined(__s390x__)
constexpr bool kHostIsLittleEndian = false;
#else
// When forcing s390x endianness on other platforms (for testing), keep the actual
// host endianness so we don't double-swap on true little-endian hosts.
constexpr bool kHostIsLittleEndian = ARROW_LITTLE_ENDIAN != 0;
#endif
#elif defined(__s390__) || defined(__s390x__)
constexpr bool kHostIsLittleEndian = false;
#else
constexpr bool kHostIsLittleEndian = ARROW_LITTLE_ENDIAN != 0;
#endif

template <typename T>
struct NeedsEndianConversion
    : std::bool_constant<!kHostIsLittleEndian &&
                         (std::is_integral_v<T> || std::is_floating_point_v<T>)> {};

template <>
struct NeedsEndianConversion<Int96> : std::bool_constant<!kHostIsLittleEndian> {};

template <>
struct NeedsEndianConversion<FixedLenByteArray> {
  static constexpr bool value = false;
};

template <typename T>
inline T ToLittleEndianValue(T v) {
  if constexpr (NeedsEndianConversion<T>::value) {
    return ByteSwap<T>::Do(v);
  }
  return v;
}

template <typename T>
inline T LoadLittleEndianScalar(const uint8_t* p) {
  T v;
  std::memcpy(&v, p, sizeof(T));
  if constexpr (NeedsEndianConversion<T>::value) {
    v = ByteSwap<T>::Do(v);
  }
  return v;
}

template <typename T>
inline void DecodeValues(const uint8_t* src, int64_t n, T* out) {
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
  if (n <= 0) return;
  if constexpr (NeedsEndianConversion<T>::value) {
    for (int64_t i = 0; i < n; ++i) {
      T v;
      std::memcpy(&v, src + i * static_cast<int64_t>(sizeof(T)), sizeof(T));
      out[i] = ByteSwap<T>::Do(v);
    }
    return;
  }
  std::memcpy(out, src, static_cast<size_t>(n) * sizeof(T));
}

template <typename T>
inline const uint8_t* PrepareLittleEndianBuffer(const T* in, int64_t n,
                                                std::vector<T>* scratch) {
  if constexpr (NeedsEndianConversion<T>::value) {
    scratch->resize(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
      (*scratch)[static_cast<size_t>(i)] = ByteSwap<T>::Do(in[i]);
    }
    return reinterpret_cast<const uint8_t*>(scratch->data());
  }
  return reinterpret_cast<const uint8_t*>(in);
}

template <typename T>
inline void ConvertLittleEndianInPlace(T* buf, int64_t n) {
  if constexpr (NeedsEndianConversion<T>::value) {
    for (int64_t i = 0; i < n; ++i) {
      buf[i] = ByteSwap<T>::Do(buf[i]);
    }
  }
  (void)buf;
  (void)n;
}

}  // namespace internal
}  // namespace parquet
