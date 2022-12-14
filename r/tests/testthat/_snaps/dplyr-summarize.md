# Functions that take ... but we only accept a single arg

    Code
      compare_dplyr_binding(.input %>% summarize(distinct = n_distinct()) %>% collect(),
      tbl, warning = "0 arguments")
    Error <rlang_error>
      i In argument: `distinct = n_distinct()`.
      Caused by error in `n_distinct()`:
      ! `...` is absent, but must be supplied.

