# extract tz

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_y = tz(x), timezone_z = tz(z),
      timezone_w = tz(w), timezone_v = tz(v)) %>% collect(), df)
    Warning <simpleWarning>
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: Expression tz(z) not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

