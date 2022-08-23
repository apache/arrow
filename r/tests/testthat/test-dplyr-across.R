test_that("Can use across() within mutate()", {
  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), round)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dbl2 = dbl * 2,
        across(c(dbl, dbl2), round),
        int2 = int * 2,
        dbl = dbl + 3
        ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), list(exp, sqrt))) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), list("fun1" = round, "fun2" = sqrt))) %>%
      collect(),
    tbl
  )

  # this is valid is neither R nor Arrow
  expect_error(
    expect_warning(
      compare_dplyr_binding(
        .input %>%
          arrow_table() %>%
          mutate(across(c(dbl, dbl2), list("fun1" = round(sqrt(dbl))))) %>%
          collect(),
        tbl,
        warning = TRUE
      )
    )
  )

  # across() arguments not in default order
  compare_dplyr_binding(
    .input %>%
      mutate(across(.fns = round, c(dbl, dbl2))) %>%
      collect(),
    tbl
  )

  # across() with no columns named
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, dbl2) %>%
      mutate(across(.fns = round)) %>%
      collect(),
    tbl
  )

  # dynamic variable name
  int = c("dbl", "dbl2")
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, dbl2) %>%
      mutate(across(all_of(int), sqrt)) %>%
      collect(),
    tbl
  )

  # .names argument
  compare_dplyr_binding(
   .input %>%
      mutate(across(c(dbl, dbl2), round, .names = "{.col}.{.fn}")) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), round, .names = "{.col}.{.fn}")) %>%
      collect(),
    tbl
  )

  # ellipses (...) are a deprecated argument
  expect_error(
    tbl %>%
      arrow_table() %>%
      mutate(across(c(dbl, dbl2), round, digits = -1)) %>%
      collect(),
    regexp = "`...` argument to `across()` is deprecated in dplyr and not supported in Arrow",
    fixed = TRUE
  )

  # alternative ways of specifying .fns - as a list
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, list(round))) %>%
      collect(),
    tbl
  )

  # supply .fns as a one-item vector
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, c(round))) %>%
      collect(),
    tbl
  )

  # ARROW-17366: purrr-style lambda functions not yet supported
  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(1:dbl2, ~ round(.x, digits = -1))) %>%
        collect(),
      tbl
    ),
    regexp = "purrr-style lambda functions as `.fns` argument to `across()` not yet supported in Arrow",
    fixed = TRUE
  )

  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(1:dbl2, list(~ round(.x, digits = -1), ~sqrt(.x)))) %>%
        collect(),
      tbl
    ),
    regexp = "purrr-style lambda functions as `.fns` argument to `across()` not yet supported in Arrow",
    fixed = TRUE
  )

  # .fns = NULL, the default
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, NULL)) %>%
      collect(),
    tbl
  )

  # ARROW-12778 - `where()` is not yet supported
  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(where(is.double))) %>%
        collect(),
      tbl
    ),
    "Unsupported selection helper"
  )

  # gives the right error with window functions
  expect_warning(
    arrow_table(tbl) %>%
      mutate(
        x = int + 2,
        across(c("int", "dbl"), list(mean = mean, sd = sd, round)),
        exp(dbl2)
      ) %>%
      collect(),
    "window functions not currently supported in Arrow; pulling data into R",
    fixed = TRUE
  )
})
