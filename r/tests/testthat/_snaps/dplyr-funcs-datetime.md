# extract tz

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_posixct_date = tz(posixct_date),
      timezone_char_date = tz(char_date)) %>% collect(), df)
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(char_date), timezone extraction for objects of class `string` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_posixct_date = tz(posixct_date),
      timezone_date_date = tz(date_date)) %>% collect(), df)
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(date_date), timezone extraction for objects of class `date32[day]` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_posixct_date = tz(posixct_date),
      timezone_integer_date = tz(integer_date)) %>% collect(), df)
    Warning <simpleWarning>
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class integer; returning "UTC". This warning will become an error in the next major version of lubridate.
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(integer_date), timezone extraction for objects of class `int32` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

---

    Code
      compare_dplyr_binding(.input %>% mutate(timezone_posixct_date = tz(posixct_date),
      timezone_double_date = tz(double_date)) %>% collect(), df)
    Warning <simpleWarning>
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
      tz(): Don't know how to compute timezone for object of class numeric; returning "UTC". This warning will become an error in the next major version of lubridate.
    Error <expectation_failure>
      `via_batch <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(.input = record_batch(tbl))))` threw an unexpected warning.
      Message: In tz(double_date), timezone extraction for objects of class `double` not supported in Arrow; pulling data into R
      Class:   simpleWarning/warning/condition

