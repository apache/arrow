# expand_across correctly expands quosures

    Code
      InMemoryDataset$create(example_data) %>% mutate(across(c(dbl, dbl2), round,
      digits = -1))
    Condition
      Error in `mutate.Dataset()`:
      ! `...` argument to `across()` is deprecated in dplyr and not supported in Arrow
      > Convert your call into a function or formula including the arguments
      > Or, call collect() first to pull data into R.

