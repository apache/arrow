# left_join

    Code
      compare_dplyr_binding(.input %>% left_join(to_join) %>% collect(), left)
    Message <rlang_message>
      Joining with `by = join_by(some_grouping)`

# Error handling

    Code
      left_join(arrow_table(example_data), arrow_table(example_data), by = "made_up_colname")
    Condition
      Error in `handle_join_by()`:
      ! Join columns must be present in data.
      x `made_up_colname` not present in x.
      x `made_up_colname` not present in y.

---

    Code
      left_join(arrow_table(example_data), arrow_table(example_data), by = c(int = "made_up_colname"))
    Condition
      Error in `handle_join_by()`:
      ! Join columns must be present in data.
      x `made_up_colname` not present in y.

---

    Code
      left_join(arrow_table(example_data), arrow_table(example_data), by = c(
        made_up_colname = "int"))
    Condition
      Error in `handle_join_by()`:
      ! Join columns must be present in data.
      x `made_up_colname` not present in x.

---

    Code
      left_join(arrow_table(example_data), arrow_table(example_data), by = c(
        "made_up_colname1", "made_up_colname2"))
    Condition
      Error in `handle_join_by()`:
      ! Join columns must be present in data.
      x `made_up_colname1` and `made_up_colname2` not present in x.
      x `made_up_colname1` and `made_up_colname2` not present in y.

---

    Code
      left_join(arrow_table(example_data), arrow_table(example_data), by = c(
        made_up_colname1 = "made_up_colname2"))
    Condition
      Error in `handle_join_by()`:
      ! Join columns must be present in data.
      x `made_up_colname1` not present in x.
      x `made_up_colname2` not present in y.

