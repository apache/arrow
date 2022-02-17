# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

register_bindings_math <- function() {
  log_binding <- function(x, base = exp(1)) {
    # like other binary functions, either `x` or `base` can be Expression or double(1)
    if (is.numeric(x) && length(x) == 1) {
      x <- Expression$scalar(x)
    } else if (!inherits(x, "Expression")) {
      arrow_not_supported("x must be a column or a length-1 numeric; other values")
    }

    # handle `base` differently because we use the simpler ln, log2, and log10
    # functions for specific scalar base values
    if (inherits(base, "Expression")) {
      return(Expression$create("logb_checked", x, base))
    }

    if (!is.numeric(base) || length(base) != 1) {
      arrow_not_supported("base must be a column or a length-1 numeric; other values")
    }

    if (base == exp(1)) {
      return(Expression$create("ln_checked", x))
    }

    if (base == 2) {
      return(Expression$create("log2_checked", x))
    }

    if (base == 10) {
      return(Expression$create("log10_checked", x))
    }

    Expression$create("logb_checked", x, Expression$scalar(base))
  }

  register_binding("log", log_binding)
  register_binding("logb", log_binding)

  register_binding("pmin", function(..., na.rm = FALSE) {
    build_expr(
      "min_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  })

  register_binding("pmax", function(..., na.rm = FALSE) {
    build_expr(
      "max_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  })

  register_binding("trunc", function(x, ...) {
    # accepts and ignores ... for consistency with base::trunc()
    build_expr("trunc", x)
  })

  register_binding("round", function(x, digits = 0) {
    build_expr(
      "round",
      x,
      options = list(ndigits = digits, round_mode = RoundMode$HALF_TO_EVEN)
    )
  })
}
