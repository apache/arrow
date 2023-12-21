.. Copyright (c) 2016, Johan Mabille and Sylvain Corlay

   Distributed under the terms of the BSD 3-Clause License.

   The full license is in the file LICENSE, distributed with this software.

Writing vectorized code
=======================

Assume that we have a simple function that computes the mean of two vectors, something like:

.. code::

    #include <cstddef>
    #include <vector>

    void mean(const std::vector<double>& a, const std::vector<double>& b, std::vector<double>& res)
    {
        std::size_t size = res.size();
        for(std::size_t i = 0; i < size; ++i)
        {
            res[i] = (a[i] + b[i]) / 2;
        }
    }

How can we used `xsimd` to take advantage of vectorization ?

Explicit use of an instruction set
----------------------------------

`xsimd` provides the template class :cpp:class:`xsimd::batch` parametrized by ``T`` and ``A`` types where ``T`` is the type of the values involved in SIMD
instructions and ``A`` is the target architecture. If you know which instruction set is available on your machine, you can directly use the corresponding specialization
of ``batch``. For instance, assuming the AVX instruction set is available, the previous code can be vectorized the following way:

.. code::

    #include <cstddef>
    #include <vector>
    #include "xsimd/xsimd.hpp"

    void mean(const std::vector<double>& a, const std::vector<double>& b, std::vector<double>& res)
    {
        using b_type = xsimd::batch<double, xsimd::avx>;
        std::size_t inc = b_type::size;
        std::size_t size = res.size();
        // size for which the vectorization is possible
        std::size_t vec_size = size - size % inc;
        for(std::size_t i = 0; i < vec_size; i +=inc)
        {
            b_type avec = b_type::load_unaligned(&a[i]);
            b_type bvec = b_type::load_unaligned(&b[i]);
            b_type rvec = (avec + bvec) / 2;
            rvec.store_unaligned(&res[i]);
        }
        // Remaining part that cannot be vectorize
        for(std::size_t i = vec_size; i < size; ++i)
        {
            res[i] = (a[i] + b[i]) / 2;
        }
    }

However, if you want to write code that is portable, you cannot rely on the use of ``batch<double, xsimd::avx>``.
Indeed this won't compile on a CPU where only SSE2 instruction set is available for instance. Fortunately, if you don't set the second template parameter, `xsimd` picks the best architecture among the one available, based on the compiler flag you use.


Aligned vs unaligned memory
---------------------------

In the previous example, you may have noticed the :cpp:func:`xsimd::batch::load_unaligned` and :cpp:func:`xsimd::batch::store_unaligned` functions. These
are meant for loading values from contiguous dynamically allocated memory into SIMD registers and
reciprocally. When dealing with memory transfer operations, some instructions sets required the memory
to be aligned by a given amount, others can handle both aligned and unaligned modes. In that latter case,
operating on aligned memory is generally faster than operating on unaligned memory.

`xsimd` provides an aligned memory allocator, namely :cpp:class:`xsimd::aligned_allocator` which follows the standard requirements, so it can be used
with STL containers. Let's change the previous code so it can take advantage of this allocator:

.. code::

    #include <cstddef>
    #include <vector>
    #include "xsimd/xsimd.hpp"

    using vector_type = std::vector<double, xsimd::default_allocator<double>>;
    void mean(const vector_type& a, const vector_type& b, vector_type& res)
    {
        using b_type = xsimd::batch<double>;
        std::size_t inc = b_type::size;
        std::size_t size = res.size();
        // size for which the vectorization is possible
        std::size_t vec_size = size - size % inc;
        for(std::size_t i = 0; i < vec_size; i += inc)
        {
            b_type avec = b_type::load_aligned(&a[i]);
            b_type bvec = b_type::load_aligned(&b[i]);
            b_type rvec = (avec + bvec) / 2;
            rvec.store_aligned(&res[i]);
        }
        // Remaining part that cannot be vectorize
        for(std::size_t i = vec_size; i < size; ++i)
        {
            res[i] = (a[i] + b[i]) / 2;
        }
    }

Memory alignment and tag dispatching
------------------------------------

You may need to write code that can operate on any type of vectors or arrays, not only the STL ones. In that
case, you cannot make assumption on the memory alignment of the container. `xsimd` provides a tag dispatching
mechanism that allows you to easily write such a generic code:


.. code::

    #include <cstddef>
    #include <vector>
    #include "xsimd/xsimd.hpp"

    template <class C, class Tag>
    void mean(const C& a, const C& b, C& res, Tag)
    {
        using b_type = xsimd::batch<double>;
        std::size_t inc = b_type::size;
        std::size_t size = res.size();
        // size for which the vectorization is possible
        std::size_t vec_size = size - size % inc;
        for(std::size_t i = 0; i < vec_size; i += inc)
        {
            b_type avec = b_type::load(&a[i], Tag());
            b_type bvec = b_type::load(&b[i], Tag());
            b_type rvec = (avec + bvec) / 2;
            xsimd::store(&res[i], rvec, Tag());
        }
        // Remaining part that cannot be vectorize
        for(std::size_t i = vec_size; i < size; ++i)
        {
            res[i] = (a[i] + b[i]) / 2;
        }
    }

Here, the ``Tag`` template parameter can be :cpp:class:`xsimd::aligned_mode` or :cpp:class:`xsimd::unaligned_mode`. Assuming the existence
of a ``get_alignment_tag`` meta-function in the code, the previous code can be invoked this way:

.. code::

    mean(a, b, res, get_alignment_tag<decltype(a)>());

Writing arch-independent code
-----------------------------

If your code may target either SSE2, AVX2 or AVX512 instruction set, `xsimd`
make it possible to make your code even more generic by using the architecture
as a template parameter:


.. code::

    #include <cstddef>
    #include <vector>
    #include "xsimd/xsimd.hpp"

    struct mean {
        template <class C, class Tag, class Arch>
        void operator()(Arch, const C& a, const C& b, C& res, Tag)
        {
            using b_type = xsimd::batch<double, Arch>;
            std::size_t inc = b_type::size;
            std::size_t size = res.size();
            // size for which the vectorization is possible
            std::size_t vec_size = size - size % inc;
            for(std::size_t i = 0; i < vec_size; i += inc)
            {
                b_type avec = b_type::load(&a[i], Tag());
                b_type bvec = b_type::load(&b[i], Tag());
                b_type rvec = (avec + bvec) / 2;
                xsimd::store(&res[i], rvec, Tag());
            }
            // Remaining part that cannot be vectorize
            for(std::size_t i = vec_size; i < size; ++i)
            {
                res[i] = (a[i] + b[i]) / 2;
            }
        }
    };

This can be useful to implement runtime dispatching, based on the instruction set detected at runtime. `xsimd` provides a generic machinery :cpp:func:`xsimd::dispatch()` to implement
this pattern. Based on the above example, instead of calling ``mean{}(arch, a, b, res, tag)``, one can use ``xsimd::dispatch(mean{})(a, b, res, tag)``.
