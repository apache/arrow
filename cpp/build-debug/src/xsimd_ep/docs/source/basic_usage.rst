.. Copyright (c) 2016, Johan Mabille and Sylvain Corlay

   Distributed under the terms of the BSD 3-Clause License.

   The full license is in the file LICENSE, distributed with this software.

Basic usage
===========

Explicit use of an instruction set extension
--------------------------------------------

Here is an example that computes the mean of two sets of 4 double floating point values, using the AVX extension:

.. code::

    #include <iostream>
    #include "xsimd/xsimd.hpp"

    namespace xs = xsimd;

    int main(int argc, char* argv[])
    {
        xs::batch<double, xs::avx> a = {1.5, 2.5, 3.5, 4.5};
        xs::batch<double, xs::avx> b = {2.5, 3.5, 4.5, 5.5};
        auto mean = (a + b) / 2;
        std::cout << mean << std::endl;
        return 0;
    }

Note that in that case, the instruction set is explicilty specified in the batch type.

This example outputs:

.. code::

    (2.0, 3.0, 4.0, 5.0)

Auto detection of the instruction set extension to be used
----------------------------------------------------------

The same computation operating on vectors and using the most performant instruction set available, using a code that's generic on the batch size:

.. code::

    #include <cstddef>
    #include <vector>
    #include "xsimd/xsimd.hpp"

    namespace xs = xsimd;
    using vector_type = std::vector<double, xsimd::aligned_allocator<double>>;

    void mean(const vector_type& a, const vector_type& b, vector_type& res)
    {
        std::size_t size = a.size();
        constexpr std::size_t simd_size = xsimd::simd_type<double>::size;
        std::size_t vec_size = size - size % simd_size;

        for(std::size_t i = 0; i < vec_size; i += simd_size)
        {
            auto ba = xs::load_aligned(&a[i]);
            auto bb = xs::load_aligned(&b[i]);
            auto bres = (ba + bb) / 2;
            bres.store_aligned(&res[i]);
        }
        for(std::size_t i = vec_size; i < size; ++i)
        {
            res[i] = (a[i] + b[i]) / 2;
        }
    }

In that case, the architecture is chosen based on the compilation flags, prioritizing the largest width and the most recent instruction set.
