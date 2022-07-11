# show_exec_plan()

    Code
      tbl %>% arrow_table() %>% filter(dbl > 2, chr != "e") %>% select(chr, int, lgl) %>%
        mutate(int_plus_ten = int + 10) %>% show_exec_plan()
    Output
      ExecPlan with 3 nodes:
      2:ProjectNode{projection=[chr, int, lgl, "int_plus_ten": add_checked(cast(int, {to_type=double, allow_int_overflow=false, allow_time_truncate=false, allow_time_overflow=false, allow_decimal_truncate=false, allow_float_truncate=false, allow_invalid_utf8=false}), 10)]}
        1:FilterNode{filter=((dbl > 2) and (chr != "e"))}
          0:TableSourceNode{}

---

    Code
      tbl %>% record_batch() %>% filter(dbl > 2, chr != "e") %>% select(chr, int, lgl) %>%
        mutate(int_plus_ten = int + 10) %>% show_exec_plan()
    Output
      ExecPlan with 3 nodes:
      2:ProjectNode{projection=[chr, int, lgl, "int_plus_ten": add_checked(cast(int, {to_type=double, allow_int_overflow=false, allow_time_truncate=false, allow_time_overflow=false, allow_decimal_truncate=false, allow_float_truncate=false, allow_invalid_utf8=false}), 10)]}
        1:FilterNode{filter=((dbl > 2) and (chr != "e"))}
          0:TableSourceNode{}

