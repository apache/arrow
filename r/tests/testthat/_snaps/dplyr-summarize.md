# Functions that take ... but we only accept a single arg

    Code
      InMemoryDataset$create(tbl) %>% summarize(distinct = n_distinct())
    Condition
      Error in `n_distinct()`:
      ! n_distinct() with 0 arguments not supported in Arrow
      > Call collect() first to pull data into R.

---

    In n_distinct(int, lgl): 
    i Multiple arguments to n_distinct() not supported in Arrow
    > Pulling data into R

# Expressions on aggregations

    Code
      record_batch(tbl) %>% summarise(any(any(lgl)))
    Condition
      Warning:
      In any(any(lgl)): 
      i aggregate within aggregate expression not supported in Arrow
      > Pulling data into R
    Output
      # A tibble: 1 x 1
        `any(any(lgl))`
        <lgl>          
      1 TRUE           

# Can use across() within summarise()

    Code
      data.frame(x = 1, y = 2) %>% arrow_table() %>% group_by(x) %>% summarise(across(
        everything())) %>% collect()
    Condition
      Warning:
      In y: 
      i Expression is not a valid aggregation expression or is not supported in Arrow
      > Pulling data into R
    Output
      # A tibble: 1 x 2
            x     y
        <dbl> <dbl>
      1     1     2

