.. Copyright (c) 2016, Johan Mabille, Sylvain Corlay 

   Distributed under the terms of the BSD 3-Clause License.

   The full license is in the file LICENSE, distributed with this software.

.. raw:: html

   <style>
   .rst-content table.docutils {
       width: 100%;
       table-layout: fixed;
   }

   table.docutils .line-block {
       margin-left: 0;
       margin-bottom: 0;
   }

   table.docutils code.literal {
       color: initial;
   }

   code.docutils {
       background: initial;
   }
   </style>

.. _Arch Dispatching:

Arch Dispatching
================

`xsimd` provides a generic way to dispatch a function call based on the architecture the code was compiled for and the architectures available at runtime.
The :cpp:func:`xsimd::dispatch` function takes a functor whose call operator takes an architecture parameter as first operand, followed by any number of arguments ``Args...`` and turn it into a
dispatching functor that takes ``Args...`` as arguments.

.. doxygenfunction:: xsimd::dispatch
    :project: xsimd

Following code showcases a usage of the :cpp:func:`xsimd::dispatch` function:

.. code-block:: c++

    #include "sum.hpp"

    // Create the dispatching function, specifying the architecture we want to
    // target.
    auto dispatched = xsimd::dispatch<xsimd::arch_list<xsimd::avx2, xsimd::sse2>(sum{});

    // Call the appropriate implementation based on runtime information.
    float res = dispatched(data, 17);

This code does *not* require any architecture-specific flags. The architecture
specific details follow.

The ``sum.hpp`` header contains the function being actually called, in an
architecture-agnostic description:

.. code-block:: c++

    #ifndef _SUM_HPP
    #define _SUM_HPP

    // functor with a call method that depends on `Arch`
    struct sum {
      // It's critical not to use an in-class definition here.
      // In-class and inline definition bypass extern template mechanism.
      template<class Arch, class T>
      T operator()(Arch, T const* data, unsigned size);
    };

    template<class Arch, class T>
    T sum::operator()(Arch, T const* data, unsigned size)
    {
      using batch = xsimd::batch<T, Arch>;
      batch acc(static_cast<T>(0));
      const unsigned n = size / batch::size * batch::size;
      for(unsigned i = 0; i != n; i += batch::size)
          acc += batch::load_unaligned(data + i);
      T star_acc = xsimd::reduce_add(acc);
      for(unsigned i = n; i < size; ++i)
        star_acc += data[i];
      return star_acc;
    }

    // Inform the compiler that sse2 and avx2 implementation are to be found in another compilaton unit.
    extern template float sum::operator()<xsimd::avx2, float>(xsimd::avx2, float const*, unsigned);
    extern template float sum::operator()<xsimd::sse2, float>(xsimd::sse2, float const*, unsigned);
    #endif

The SSE2 and AVX2 version needs to be provided in other compilation units, compiled with the appropriate flags, for instance:

.. code-block:: c++

    // compile with -mavx2
    #include "sum.hpp"
    template float sum::operator()<xsimd::avx2, float>(xsimd::avx2, float const*, unsigned);

.. code-block:: c++

    // compile with -msse2
    #include "sum.hpp"
    template float sum::operator()<xsimd::sse2, float>(xsimd::sse2, float const*, unsigned);

