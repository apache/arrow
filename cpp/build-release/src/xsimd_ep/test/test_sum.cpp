#include "test_sum.hpp"
#if XSIMD_WITH_AVX
template float sum::operator()(xsimd::avx, float const*, unsigned);
#endif
