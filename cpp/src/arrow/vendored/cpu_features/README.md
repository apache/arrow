# cpu_features [![Build Status](https://travis-ci.org/google/cpu_features.svg?branch=master)](https://travis-ci.org/google/cpu_features) [![Build status](https://ci.appveyor.com/api/projects/status/46d1owsj7n8dsylq/branch/master?svg=true)](https://ci.appveyor.com/project/gchatelet/cpu-features/branch/master)

A cross-platform C library to retrieve CPU features (such as available
instructions) at runtime.

## Table of Contents

- [Design Rationale](#rationale)
- [Code samples](#codesample)
- [Running sample code](#usagesample)
- [What's supported](#support)
- [Android NDK's drop in replacement](#ndk)
- [License](#license)
- [Build with cmake](#cmake)

<a name="rationale"></a>
## Design Rationale

-   **Simple to use.** See the snippets below for examples.
-   **Extensible.** Easy to add missing features or architectures.
-   **Compatible with old compilers** and available on many architectures so it
    can be used widely. To ensure that cpu_features works on as many platforms
    as possible, we implemented it in a highly portable version of C: C99.
-   **Sandbox-compatible.** The library uses a variety of strategies to cope
    with sandboxed environments or when `cpuid` is unavailable. This is useful
    when running integration tests in hermetic environments.
-   **Thread safe, no memory allocation, and raises no exceptions.**
    cpu_features is suitable for implementing fundamental libc functions like
    `malloc`, `memcpy`, and `memcmp`.
-   **Unit tested.**

<a name="codesample"></a>
### Checking features at runtime

Here's a simple example that executes a codepath if the CPU supports both the
AES and the SSE4.2 instruction sets:

```c
#include "cpuinfo_x86.h"

static const X86Features features = GetX86Info().features;

void Compute(void) {
  if (features.aes && features.sse4_2) {
    // Run optimized code.
  } else {
    // Run standard code.
  }
}
```

### Caching for faster evaluation of complex checks

If you wish, you can read all the features at once into a global variable, and
then query for the specific features you care about. Below, we store all the ARM
features and then check whether AES and NEON are supported.

```c
#include <stdbool.h>
#include "cpuinfo_arm.h"

static const ArmFeatures features = GetArmInfo().features;
static const bool has_aes_and_neon = features.aes && features.neon;

// use has_aes_and_neon.
```

This is a good approach to take if you're checking for combinations of features
when using a compiler that is slow to extract individual bits from bit-packed
structures.

### Checking compile time flags

The following code determines whether the compiler was told to use the AVX
instruction set (e.g., `g++ -mavx`) and sets `has_avx` accordingly.

```c
#include <stdbool.h>
#include "cpuinfo_x86.h"

static const X86Features features = GetX86Info().features;
static const bool has_avx = CPU_FEATURES_COMPILED_X86_AVX || features.avx;

// use has_avx.
```

`CPU_FEATURES_COMPILED_X86_AVX` is set to 1 if the compiler was instructed to
use AVX and 0 otherwise, combining compile time and runtime knowledge.

### Rejecting poor hardware implementations based on microarchitecture

On x86, the first incarnation of a feature in a microarchitecture might not be
the most efficient (e.g. AVX on Sandy Bridge). We provide a function to retrieve
the underlying microarchitecture so you can decide whether to use it.

Below, `has_fast_avx` is set to 1 if the CPU supports the AVX instruction
set&mdash;but only if it's not Sandy Bridge.

```c
#include <stdbool.h>
#include "cpuinfo_x86.h"

static const X86Info info = GetX86Info();
static const X86Microarchitecture uarch = GetX86Microarchitecture(&info);
static const bool has_fast_avx = info.features.avx && uarch != INTEL_SNB;

// use has_fast_avx.
```

This feature is currently available only for x86 microarchitectures.

<a name="usagesample"></a>
### Running sample code

Building `cpu_features` brings a small executable to test the library.

```shell
 % ./build/list_cpu_features
arch            : x86
brand           :        Intel(R) Xeon(R) CPU E5-1650 0 @ 3.20GHz
family          :   6 (0x06)
model           :  45 (0x2D)
stepping        :   7 (0x07)
uarch           : INTEL_SNB
flags           : aes,avx,cx16,smx,sse4_1,sse4_2,ssse3
```

```shell
% ./build/list_cpu_features --json
{"arch":"x86","brand":"       Intel(R) Xeon(R) CPU E5-1650 0 @ 3.20GHz","family":6,"model":45,"stepping":7,"uarch":"INTEL_SNB","flags":["aes","avx","cx16","smx","sse4_1","sse4_2","ssse3"]}
```

<a name="support"></a>
## What's supported

|         | x86³ |   ARM   | AArch64 |  MIPS⁴ |  POWER  |
|---------|:----:|:-------:|:-------:|:------:|:-------:|
| Android | yes² |   yes¹  |   yes¹  |  yes¹  |   N/A   |
| iOS     |  N/A | not yet | not yet |   N/A  |   N/A   |
| Linux   | yes² |   yes¹  |   yes¹  |  yes¹  |   yes¹  |
| MacOs   | yes² |   N/A   | not yet |   N/A  |    no   |
| Windows | yes² | not yet | not yet |   N/A  |   N/A   |

1.  **Features revealed from Linux.** We gather data from several sources
    depending on availability:
    +   from glibc's
        [getauxval](https://www.gnu.org/software/libc/manual/html_node/Auxiliary-Vector.html)
    +   by parsing `/proc/self/auxv`
    +   by parsing `/proc/cpuinfo`
2.  **Features revealed from CPU.** features are retrieved by using the `cpuid`
    instruction.
3.  **Microarchitecture detection.** On x86 some features are not always
    implemented efficiently in hardware (e.g. AVX on Sandybridge). Exposing the
    microarchitecture allows the client to reject particular microarchitectures.
4.  All flavors of Mips are supported, little and big endian as well as 32/64
    bits.

<a name="ndk"></a>
## Android NDK's drop in replacement

[cpu_features](https://github.com/google/cpu_features) is now officially
supporting Android and offers a drop in replacement of for the NDK's [cpu-features.h](https://android.googlesource.com/platform/ndk/+/master/sources/android/cpufeatures/cpu-features.h)
, see [ndk_compat](ndk_compat) folder for details.

<a name="license"></a>
## License

The cpu_features library is licensed under the terms of the Apache license.
See [LICENSE](LICENSE) for more information.

<a name="cmake"></a>
## Build with CMake

Please check the [CMake build instructions](cmake/README.md).
