# paste, paste0, and str_c

    Code
      (expect_error(nse_funcs$paste(x, y, sep = NA_character_)))
    Output
      <assertError: Invalid separator>

---

    Code
      (expect_error(nse_funcs$paste(x, y, collapse = "")))
    Output
      <assertError: paste() with the collapse argument is not yet supported in Arrow>
    Code
      (expect_error(nse_funcs$paste0(x, y, collapse = "")))
    Output
      <assertError: paste0() with the collapse argument is not yet supported in Arrow>
    Code
      (expect_error(nse_funcs$str_c(x, y, collapse = "")))
    Output
      <assertError: str_c() with the collapse argument is not yet supported in Arrow>
    Code
      (expect_error(nse_funcs$paste(x, character(0), y)))
    Output
      <assertError: Literal vectors of length != 1 not supported in string concatenation>
    Code
      (expect_error(nse_funcs$paste(x, c(",", ";"), y)))
    Output
      <assertError: Literal vectors of length != 1 not supported in string concatenation>

# str_to_lower, str_to_upper, and str_to_title

    Code
      (expect_error(nse_funcs$str_to_lower("Apache Arrow", locale = "sp")))
    Output
      <simpleError: Providing a value for 'locale' other than the default ('en') is not supported by Arrow. To change locale, use 'Sys.setlocale()'>

# errors and warnings in string splitting

    Code
      (expect_error(nse_funcs$str_split(x, fixed("and", ignore_case = TRUE))))
    Output
      <simpleError: Case-insensitive string splitting not supported by Arrow>
    Code
      (expect_error(nse_funcs$str_split(x, coll("and.?"))))
    Output
      <simpleError: Pattern modifier `coll()` not supported by Arrow>
    Code
      (expect_error(nse_funcs$str_split(x, boundary(type = "word"))))
    Output
      <simpleError: Pattern modifier `boundary()` not supported by Arrow>
    Code
      (expect_error(nse_funcs$str_split(x, "and", n = 0)))
    Output
      <simpleError: Splitting strings into zero parts not supported by Arrow>
    Code
      (expect_warning(nse_funcs$str_split(x, fixed("and"), simplify = TRUE)))
    Output
      <simpleWarning: Argument 'simplify = TRUE' will be ignored>

# errors and warnings in string detection and replacement

    Code
      (expect_error(nse_funcs$str_detect(x, boundary(type = "character"))))
    Output
      <simpleError: Pattern modifier `boundary()` not supported by Arrow>
    Code
      (expect_error(nse_funcs$str_replace_all(x, coll("o", locale = "en"), "ó")))
    Output
      <simpleError: Pattern modifier `coll()` not supported by Arrow>
    Code
      (expect_warning(nse_funcs$str_replace_all(x, regex("o", multiline = TRUE), "u"))
      )
    Output
      <simpleWarning: Ignoring pattern modifier argument not supported in Arrow: "multiline">

# errors in strptime

    Code
      (expect_error(nse_funcs$strptime(x, tz = "PDT")))
    Output
      <simpleError: Time zone argument not supported by Arrow>

# substr

    Code
      (expect_error(nse_funcs$substr("Apache Arrow", c(1, 2), 3)))
    Output
      <assertError: `start` must be length 1 - other lengths are not supported in Arrow>
    Code
      (expect_error(nse_funcs$substr("Apache Arrow", 1, c(2, 3))))
    Output
      <assertError: `stop` must be length 1 - other lengths are not supported in Arrow>

# str_sub

    Code
      (expect_error(nse_funcs$str_sub("Apache Arrow", c(1, 2), 3)))
    Output
      <assertError: `start` must be length 1 - other lengths are not supported in Arrow>
    Code
      (expect_error(nse_funcs$str_sub("Apache Arrow", 1, c(2, 3))))
    Output
      <assertError: `end` must be length 1 - other lengths are not supported in Arrow>

