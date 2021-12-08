

register_math_translations <- function() {

  nse_funcs$log <- nse_funcs$logb <- function(x, base = exp(1)) {
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


  nse_funcs$pmin <- function(..., na.rm = FALSE) {
    build_expr(
      "min_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  }

  nse_funcs$pmax <- function(..., na.rm = FALSE) {
    build_expr(
      "max_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  }

  nse_funcs$trunc <- function(x, ...) {
    # accepts and ignores ... for consistency with base::trunc()
    build_expr("trunc", x)
  }

  nse_funcs$round <- function(x, digits = 0) {
    build_expr(
      "round",
      x,
      options = list(ndigits = digits, round_mode = RoundMode$HALF_TO_EVEN)
    )
  }
}
