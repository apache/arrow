# extract tz

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_y = tz(x), timezone_z = tz(y)) %>%
        collect(), df)
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(y), timezone extraction for objects of class `character` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_y = tz(x), timezone_z = tz(z)) %>%
        collect(), df)
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(z), timezone extraction for objects of class `date` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_y = tz(x), timezone_w = tz(w)) %>%
        collect(), df)
    Warning <simpleWarning>
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(w), timezone extraction for objects of class `numeric` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_y = tz(x), timezone_v = tz(v)) %>%
        collect(), df)
    Warning <simpleWarning>
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(v), timezone extraction for objects of class `numeric` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

