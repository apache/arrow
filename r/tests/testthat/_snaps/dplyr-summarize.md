# Functions that take ... but we only accept a single arg

    Code
      InMemoryDataset$create(tbl) %>% summarize(distinct = n_distinct())
    Condition
      Error:
      ! Error : In n_distinct(), n_distinct() with 0 arguments not supported in Arrow
      Call collect() first to pull data into R.

---

    Error : In n_distinct(int, lgl), Multiple arguments to n_distinct() not supported in Arrow; pulling data into R

