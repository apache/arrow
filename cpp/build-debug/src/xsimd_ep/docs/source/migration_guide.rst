.. Copyright (c) 2016, Johan Mabille and Sylvain Corlay

   Distributed under the terms of the BSD 3-Clause License.

   The full license is in the file LICENSE, distributed with this software.


.. raw:: html

   <style>
   .rst-content .section>img {
       width: 30px;
       margin-bottom: 0;
       margin-top: 0;
       margin-right: 15px;
       margin-left: 15px;
       float: left;
   }
   </style>

From 7.x to 8.x
===============

Version 8.x introduces a lot of API difference compared to version 7.x. This
section motivates the version bump and details the most notable changes.

Why 8.x
-------

Version 8.x introduces a new concept in `xsimd`: all batch types are now
parametrized by a type, say ``double``, and an optional architecture, say
``avx512``, as in ``batch<double, avx512>``. It is still possible to just
require a batch of doubles and let the library pick the most appropriate
architecture, as in ``batch<double>``.

This new design make it possible to target multiple architecture from the same
code, as detailed in the :ref:`Arch Dispatching` section.

As a side effect of this (almost full) rewrite of the library code, `xsimd` is
now twice as fast to compile, and its source code size as been (roughly) divided
by two. The `xsimd` developers also took this as an opportnuity to significantly
improve test coverage.

Most Notable Changes
--------------------

Batch Types
***********

The second argument of :cpp:class:`xsimd::batch` is now a type that represents
an architecture, instead of an integer.

The previous behavior can be emulated through the
:cpp:class:`xsimd::make_sized_batch` utility.

Batch of Complex Types
**********************

Loading a batch of complex from an ``xtl::xcomplex<T>`` now yields an
``xsimd::batch<std::complex<T>>`` instead of an ``xtl::xcomplex<T>``. It is still
possible to store an ``xsimd::batch<std::complex<T>>`` to an
``xtl::xcomplex<T>``.


Loading Batches
***************

``xsimd::batch<T>::load*`` are now static functions. It is no longer supported
to update an existing batch through its ``load`` method. The regular assign
operator can be used instead.

Indexing Batches
****************

``xsimd::batch<T>::operator[](size_t)`` has been replaced with
``xsimd::batch<T>::get(size_t)``. Keep in mind that this method implies a register
load *for each call*, so it's wise not to use it in performance-critical
section. When needed, do an explicit store of the batch into an array and work
from there.

Architecture Detection
**********************

Many macros have been replaced by more elaborated constructs.
``XSIMD_INSTR_SET_AVAILABLE`` has been replaced by the type alias ``xsimd::default_arch``.

Likewise architecture-specific macros like ``XSIMD_X86_INSTR_SET_AVAILABLE`` has
been replaced by ``xsimd::upported_architectures::contains<xsimd::sse3>()``. Macro like ``XSIMD_WITH_SSE3`` are still
defined to ``0`` or ``1`` to guard architecture-specific code.

