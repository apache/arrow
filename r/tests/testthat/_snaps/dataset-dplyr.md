# dplyr method not implemented messages

    Code
      filter(ds, int > 6, dbl > max(dbl))
    Condition
      Error in `dbl > max(dbl)`:
      ! Expression not supported in filter() in Arrow
      > Call collect() first to pull data into R.

