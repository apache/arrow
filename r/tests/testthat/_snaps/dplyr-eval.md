# try_arrow_dplyr/abandon_ship adds the right message about collect()

    Code
      tester(ds, i)
    Condition
      Error in `validation_error()`:
      ! arg is 0

---

    Code
      tester(ds, i)
    Condition
      Error in `arrow_not_supported()`:
      ! arg == 1 not supported in Arrow
      > Call collect() first to pull data into R.

---

    Code
      tester(ds, i)
    Condition
      Error in `arrow_not_supported()`:
      ! arg greater than 0 not supported in Arrow
      > Try setting arg to -1
      > Or, call collect() first to pull data into R.

