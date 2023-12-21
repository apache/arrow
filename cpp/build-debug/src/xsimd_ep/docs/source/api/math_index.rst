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


Mathematical functions
======================

.. toctree::

   basic_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`abs <abs-function-reference>`   | absolute value                                     |
+---------------------------------------+----------------------------------------------------+
| :ref:`fabs <fabs-function-reference>` | absolute value                                     |
+---------------------------------------+----------------------------------------------------+
| :ref:`fmod <fmod-function-reference>` | remainder of the floating point division operation |
+---------------------------------------+----------------------------------------------------+
| :ref:`remainder <remainder-func-ref>` | signed remainder of the division operation         |
+---------------------------------------+----------------------------------------------------+
| :ref:`fma <fma-function-reference>`   | fused multiply-add operation                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`fms <fms-function-reference>`   | fused multiply-sub operation                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`fnma <fnma-function-reference>` | fused negated multiply-add operation               |
+---------------------------------------+----------------------------------------------------+
| :ref:`fnms <fnms-function-reference>` | fused negated multiply-sub operation               |
+---------------------------------------+----------------------------------------------------+
| :ref:`min <min-function-reference>`   | smaller of two batches                             |
+---------------------------------------+----------------------------------------------------+
| :ref:`max <max-function-reference>`   | larger of two batches                              |
+---------------------------------------+----------------------------------------------------+
| :ref:`fmin <fmin-function-reference>` | smaller of two batches of floating point values    |
+---------------------------------------+----------------------------------------------------+
| :ref:`fmax <fmax-function-reference>` | larger of two batches of floating point values     |
+---------------------------------------+----------------------------------------------------+
| :ref:`fdim <fdim-function-reference>` | positive difference                                |
+---------------------------------------+----------------------------------------------------+
| :ref:`sadd <sadd-function-reference>` | saturated addition                                 |
+---------------------------------------+----------------------------------------------------+
| :ref:`ssub <ssub-function-reference>` | saturated subtraction                              |
+---------------------------------------+----------------------------------------------------+
| :ref:`clip <clip-function-reference>` | clipping operation                                 |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   exponential_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`exp <exp-function-reference>`   | natural exponential function                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`exp2 <exp2-function-reference>` | base 2 exponential function                        |
+---------------------------------------+----------------------------------------------------+
| :ref:`exp10 <exp10-func-ref>`         | base 10 exponential function                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`expm1 <expm1-func-ref>`         | natural exponential function, minus one            |
+---------------------------------------+----------------------------------------------------+
| :ref:`log <log-function-reference>`   | natural logarithm function                         |
+---------------------------------------+----------------------------------------------------+
| :ref:`log2 <log2-function-reference>` | base 2 logarithm function                          |
+---------------------------------------+----------------------------------------------------+
| :ref:`log10 <log10-func-ref>`         | base 10 logarithm function                         |
+---------------------------------------+----------------------------------------------------+
| :ref:`log1p <log1p-func-ref>`         | natural logarithm of one plus function             |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   power_functions

+-----------------------------------------+----------------------------------------------------+
| :ref:`pow <pow-function-reference>`     | power function                                     |
+-----------------------------------------+----------------------------------------------------+
| :ref:`rsqrt <rsqrt-function-reference>` | reciprocal square root function                    |
+-----------------------------------------+----------------------------------------------------+
| :ref:`sqrt <sqrt-function-reference>`   | square root function                               |
+-----------------------------------------+----------------------------------------------------+
| :ref:`cbrt <cbrt-function-reference>`   | cubic root function                                |
+-----------------------------------------+----------------------------------------------------+
| :ref:`hypot <hypot-func-ref>`           | hypotenuse function                                |
+-----------------------------------------+----------------------------------------------------+

.. toctree::

   trigonometric_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`sin <sin-function-reference>`   | sine function                                      |
+---------------------------------------+----------------------------------------------------+
| :ref:`cos <cos-function-reference>`   | cosine function                                    |
+---------------------------------------+----------------------------------------------------+
| :ref:`sincos <sincos-func-ref>`       | sine and cosine function                           |
+---------------------------------------+----------------------------------------------------+
| :ref:`tan <tan-function-reference>`   | tangent function                                   |
+---------------------------------------+----------------------------------------------------+
| :ref:`asin <asin-function-reference>` | arc sine function                                  |
+---------------------------------------+----------------------------------------------------+
| :ref:`acos <acos-function-reference>` | arc cosine function                                |
+---------------------------------------+----------------------------------------------------+
| :ref:`atan <atan-function-reference>` | arc tangent function                               |
+---------------------------------------+----------------------------------------------------+
| :ref:`atan2 <atan2-func-ref>`         | arc tangent function, determining quadrants        |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   hyperbolic_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`sinh <sinh-function-reference>` | hyperbolic sine function                           |
+---------------------------------------+----------------------------------------------------+
| :ref:`cosh <cosh-function-reference>` | hyperbolic cosine function                         |
+---------------------------------------+----------------------------------------------------+
| :ref:`tanh <tanh-function-reference>` | hyperbolic tangent function                        |
+---------------------------------------+----------------------------------------------------+
| :ref:`asinh <asinh-func-ref>`         | inverse hyperbolic sine function                   |
+---------------------------------------+----------------------------------------------------+
| :ref:`acosh <acosh-func-ref>`         | inverse hyperbolic cosine function                 |
+---------------------------------------+----------------------------------------------------+
| :ref:`atanh <atanh-func-ref>`         | inverse hyperbolic tangent function                |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   error_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`erf <erf-function-reference>`   | error function                                     |
+---------------------------------------+----------------------------------------------------+
| :ref:`erfc <erfc-function-reference>` | complementary error function                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`tgamma <tgamma-func-ref>`       | gamma function                                     |
+---------------------------------------+----------------------------------------------------+
| :ref:`lgamma <lgamma-func-ref>`       | natural logarithm of the gamma function            |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   nearint_operations

+---------------------------------------+----------------------------------------------------+
| :ref:`ceil <ceil-function-reference>` | nearest integers not less                          |
+---------------------------------------+----------------------------------------------------+
| :ref:`floor <floor-func-ref>`         | nearest integers not greater                       |
+---------------------------------------+----------------------------------------------------+
| :ref:`trunc <trunc-func-ref>`         | nearest integers not greater in magnitude          |
+---------------------------------------+----------------------------------------------------+
| :ref:`round <round-func-ref>`         | nearest integers, rounding away from zero          |
+---------------------------------------+----------------------------------------------------+
| :ref:`nearbyint <nearbyint-func-ref>` | nearest integers using current rounding mode       |
+---------------------------------------+----------------------------------------------------+
| :ref:`rint <rint-function-reference>` | nearest integers using current rounding mode       |
+---------------------------------------+----------------------------------------------------+

.. toctree::

   classification_functions

+---------------------------------------+----------------------------------------------------+
| :ref:`isfinite <isfinite-func-ref>`   | Checks for finite values                           |
+---------------------------------------+----------------------------------------------------+
| :ref:`isinf <isinf-func-ref>`         | Checks for infinite values                         |
+---------------------------------------+----------------------------------------------------+
| :ref:`isnan <isnan-func-ref>`         | Checks for NaN values                              |
+---------------------------------------+----------------------------------------------------+
