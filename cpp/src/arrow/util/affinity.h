#pragma once

#include <thread>

#ifdef __linux__
#include <sched.h>
#include <unistd.h>
#endif

namespace arrow {
namespace internal {

// Returns the number of CPUs the current process is allowed to use.
// Falls back to std::thread::hardware_concurrency() if affinity is not available.
inline int GetAffinityCpuCount() {
#ifdef __linux__
  cpu_set_t mask;
  if (sched_getaffinity(0, sizeof(mask), &mask) == 0) {
    return CPU_COUNT(&mask);
  }
#endif
  return std::thread::hardware_concurrency();
}

}  // namespace internal
}  // namespace arrow
