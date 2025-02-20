// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/math_internal.h"

#include <cmath>

#include "arrow/util/logging.h"

namespace arrow::internal {

double NormalPPF(double p) {
  DCHECK(p >= 0.0 && p <= 1.0);
  if (p == 0.0) {
    return -HUGE_VAL;
  }
  if (p == 1.0) {
    return HUGE_VAL;
  }

  // Algorithm from https://doi.org/10.2307/2347330
  // Wichura, M. J. (1988).
  // Algorithm AS 241: The Percentage Points of the Normal Distribution.
  // Journal of the Royal Statistical Society. Series C (Applied Statistics),
  // 37(3), 477-484.
  //
  // Copied from the Rust implementation at https://github.com/ankane/dist-rust/
  double q = p - 0.5;
  if (std::abs(q) < 0.425) {
    double r = 0.180625 - q * q;
    return q *
           (((((((2.5090809287301226727e3 * r + 3.3430575583588128105e4) * r +
                 6.7265770927008700853e4) *
                    r +
                4.5921953931549871457e4) *
                   r +
               1.3731693765509461125e4) *
                  r +
              1.9715909503065514427e3) *
                 r +
             1.3314166789178437745e2) *
                r +
            3.3871328727963666080e0) /
           (((((((5.2264952788528545610e3 * r + 2.8729085735721942674e4) * r +
                 3.9307895800092710610e4) *
                    r +
                2.1213794301586595867e4) *
                   r +
               5.3941960214247511077e3) *
                  r +
              6.8718700749205790830e2) *
                 r +
             4.2313330701600911252e1) *
                r +
            1.0);
  } else {
    double r = q < 0.0 ? p : 1.0 - p;
    r = std::sqrt(-std::log(r));
    if (r < 5.0) {
      r -= 1.6;
      r = (((((((7.74545014278341407640e-4 * r + 2.27238449892691845833e-2) * r +
                2.41780725177450611770e-1) *
                   r +
               1.27045825245236838258e0) *
                  r +
              3.64784832476320460504e0) *
                 r +
             5.76949722146069140550e0) *
                r +
            4.63033784615654529590e0) *
               r +
           1.42343711074968357734e0) /
          (((((((1.05075007164441684324e-9 * r + 5.47593808499534494600e-4) * r +
                1.51986665636164571966e-2) *
                   r +
               1.48103976427480074590e-1) *
                  r +
              6.89767334985100004550e-1) *
                 r +
             1.67638483018380384940e0) *
                r +
            2.05319162663775882187e0) *
               r +
           1.0);
    } else {
      r -= 5.0;
      r = (((((((2.01033439929228813265e-7 * r + 2.71155556874348757815e-5) * r +
                1.24266094738807843860e-3) *
                   r +
               2.65321895265761230930e-2) *
                  r +
              2.96560571828504891230e-1) *
                 r +
             1.78482653991729133580e0) *
                r +
            5.46378491116411436990e0) *
               r +
           6.65790464350110377720e0) /
          (((((((2.04426310338993978564e-15 * r + 1.42151175831644588870e-7) * r +
                1.84631831751005468180e-5) *
                   r +
               7.86869131145613259100e-4) *
                  r +
              1.48753612908506148525e-2) *
                 r +
             1.36929880922735805310e-1) *
                r +
            5.99832206555887937690e-1) *
               r +
           1.0);
    }
    return std::copysign(r, q);
  }
}

}  // namespace arrow::internal
