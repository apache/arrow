.. Copyright (c) 2016, Johan Mabille and Sylvain Corlay

   Distributed under the terms of the BSD 3-Clause License.

   The full license is in the file LICENSE, distributed with this software.

.. image:: xsimd.svg
   :alt: xsimd

C++ wrappers for SIMD intrinsics.

Introduction
------------

SIMD (Single Instruction, Multiple Data) is a feature of microprocessors that has been available for many years. SIMD instructions perform a single operation
on a batch of values at once, and thus provide a way to significantly accelerate code execution. However, these instructions differ between microprocessor
vendors and compilers.

`xsimd` provides a unified means for using these features for library authors. Namely, it enables manipulation of batches of numbers with the same arithmetic
operators as for single values. It also provides accelerated implementation of common mathematical functions operating on batches.

`xsimd` makes it easy to write a single algorithm, generate one version of the algorithm per micro-architecture and pick the best one at runtime, based on the
running processor capability.

You can find out more about this implementation of C++ wrappers for SIMD intrinsics at the `The C++ Scientist`_. The mathematical functions are a
lightweight implementation of the algorithms also used in `boost.SIMD`_.

`xsimd` requires a C++11 compliant compiler. The following C++ compilers are supported:

+-------------------------+-------------------------------+
| Compiler                | Version                       |
+=========================+===============================+
| Microsoft Visual Studio | MSVC 2015 update 2 and above  |
+-------------------------+-------------------------------+
| g++                     | 4.9 and above                 |
+-------------------------+-------------------------------+
| clang                   | 3.7 and above                 |
+-------------------------+-------------------------------+

The following SIMD instruction set extensions are supported:

+--------------+---------------------------------------------------------+
| Architecture | Instruction set extensions                              |
+==============+=========================================================+
| x86          | SSE, SSE2, SSE3, SSSE3, SSE4.1, SSE4.2, AVX, FMA3, AVX2 |
+--------------+---------------------------------------------------------+
| x86          | AVX512 (gcc7 and higher)                                |
+--------------+---------------------------------------------------------+
| x86 AMD      | same as above + FMA4                                    |
+--------------+---------------------------------------------------------+
| ARM          | ARMv7, ARMv8                                            |
+--------------+---------------------------------------------------------+

Licensing
---------

We use a shared copyright model that enables all contributors to maintain the
copyright on their contributions.

This software is licensed under the BSD-3-Clause license. See the LICENSE file for details.


.. toctree::
   :caption: INSTALLATION
   :maxdepth: 2

   installation

.. toctree::
   :caption: USAGE
   :maxdepth: 2

   basic_usage
   vectorized_code

.. toctree::
   :caption: MIGRATION GUIDE
   :maxdepth: 1

   migration_guide


.. toctree::
   :caption: API REFERENCE
   :maxdepth: 2

   api/instr_macros
   api/batch_index
   api/data_transfer
   api/arithmetic_index
   api/comparison_index
   api/bitwise_operators_index
   api/math_index
   api/reducer_index
   api/cast_index
   api/misc_index
   api/batch_manip
   api/aligned_allocator
   api/arch
   api/dispatching

.. _The C++ Scientist: http://johanmabille.github.io/blog/archives/
.. _boost.SIMD: https://github.com/NumScale/boost.simd

