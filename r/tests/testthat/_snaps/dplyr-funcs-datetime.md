# `as.Date()` and `as_date()`

    Code
      collect(transmute(InMemoryDataset$create(test_df), date_char_ymd = as.Date(
        character_ymd_var, tryFormats = c("%Y-%m-%d", "%Y/%m/%d"))))
    Condition
      Error in `as.Date()`:
      ! `as.Date()` with multiple `tryFormats` not supported in Arrow
      > Consider using the lubridate specialised parsing functions `ymd()`, `ymd()`, etc.
      > Or, call collect() first to pull data into R.

