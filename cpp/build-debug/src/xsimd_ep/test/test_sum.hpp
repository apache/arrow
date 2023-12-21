#ifndef XSIMD_TEST_SUM_HPP
#define XSIMD_TEST_SUM_HPP
#include "xsimd/xsimd.hpp"
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE

struct sum
{
    // NOTE: no inline definition here otherwise extern template instantiation
    // doesn't prevent implicit instantiation.
    template <class Arch, class T>
    T operator()(Arch, T const* data, unsigned size);
};

template <class Arch, class T>
T sum::operator()(Arch, T const* data, unsigned size)
{
    using batch = xsimd::batch<T, Arch>;
    batch acc(static_cast<T>(0));
    const unsigned n = size / batch::size * batch::size;
    for (unsigned i = 0; i != n; i += batch::size)
        acc += batch::load_unaligned(data + i);
    T star_acc = xsimd::reduce_add(acc);
    for (unsigned i = n; i < size; ++i)
        star_acc += data[i];
    return star_acc;
}

#if XSIMD_WITH_AVX
extern template float sum::operator()(xsimd::avx, float const*, unsigned);
#endif

#endif

#endif
