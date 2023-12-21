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

Available wrappers
==================

The :ref:`batch <xsimd-batch-ref>` and :ref:`batch_bool <xsimd-batch-bool-ref>` generic template classes are not implemented
by default, only full specializations of these templates are available depending on the instruction set macros defined
according to the instruction sets provided by the compiler.

XTL complex support
-------------------

If the preprocessor token ``XSIMD_ENABLE_XTL_COMPLEX`` is defined, ``xsimd``
provides batches for ``xtl::xcomplex``, similar to those for ``std::complex``.
This requires ``xtl`` to be installed.

