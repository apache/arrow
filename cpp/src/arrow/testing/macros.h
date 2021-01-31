#pragma once

#ifdef __GNUC__
#define SUPPRESS_DEPRECATION_WARNING \
  _Pragma("GCC diagnostic push");    \
  _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define UNSUPPRESS_DEPRECATION_WARNING _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
#define SUPPRESS_DEPRECATION_WARNING \
  __pragma("warning(push)");         \
  _pragma("warning(disable : 4996)")
#define UNSUPPRESS_DEPRECATION_WARNING __pragma("warning(pop)")
#endif
