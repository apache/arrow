#  show_query() and explain()

    Code
      mtcars %>% arrow_table() %>% show_query()
    Output
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          0:TableSourceNode{}

---

    Code
      tbl %>% arrow_table() %>% filter(dbl > 2, chr != "e") %>% select(chr, int, lgl) %>%
        mutate(int_plus_ten = int + 10) %>% show_query()
    Output
      ExecPlan with 4 nodes:
      3:SinkNode{}
        2:ProjectNode{projection=[chr, int, lgl, "int_plus_ten": add_checked(cast(int, {to_type=double, allow_int_overflow=false, allow_time_truncate=false, allow_time_overflow=false, allow_decimal_truncate=false, allow_float_truncate=false, allow_invalid_utf8=false}), 10)]}
          1:FilterNode{filter=((dbl > 2) and (chr != "e"))}
            0:TableSourceNode{}

---

    Code
      tbl %>% arrow_table() %>% left_join(example_data %>% arrow_table() %>% mutate(
        doubled_dbl = dbl * 2) %>% select(int, doubled_dbl), by = "int") %>% select(
        int, verses, doubled_dbl) %>% show_query()
    Output
      ExecPlan with 7 nodes:
      6:SinkNode{}
        5:ProjectNode{projection=[int, verses, doubled_dbl]}
          4:HashJoinNode{}
            3:ProjectNode{projection=[int, "doubled_dbl": multiply_checked(dbl, 2)]}
              2:TableSourceNode{}
            1:ProjectNode{projection=[int, dbl, dbl2, lgl, false, chr, fct, verses, padded_strings, another_chr]}
              0:TableSourceNode{}

---

    Code
      tbl %>% arrow_table() %>% left_join(example_data %>% arrow_table() %>% mutate(
        doubled_dbl = dbl * 2) %>% select(int, doubled_dbl), by = "int") %>% select(
        int, verses, doubled_dbl) %>% explain()
    Output
      ExecPlan with 7 nodes:
      6:SinkNode{}
        5:ProjectNode{projection=[int, verses, doubled_dbl]}
          4:HashJoinNode{}
            3:ProjectNode{projection=[int, "doubled_dbl": multiply_checked(dbl, 2)]}
              2:TableSourceNode{}
            1:ProjectNode{projection=[int, dbl, dbl2, lgl, false, chr, fct, verses, padded_strings, another_chr]}
              0:TableSourceNode{}

---

    Code
      mtcars %>% arrow_table() %>% filter(mpg > 20) %>% arrange(desc(wt)) %>%
        show_query()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={sort_keys=[FieldRef.Name(wt) DESC], null_placement=AtEnd}}
        2:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          1:FilterNode{filter=(mpg > 20)}
            0:TableSourceNode{}

---

    Code
      mtcars %>% arrow_table() %>% filter(mpg > 20) %>% arrange(desc(wt)) %>% explain()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={sort_keys=[FieldRef.Name(wt) DESC], null_placement=AtEnd}}
        2:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          1:FilterNode{filter=(mpg > 20)}
            0:TableSourceNode{}

---

    Code
      mtcars %>% arrow_table() %>% filter(mpg > 20) %>% arrange(desc(wt)) %>% head(3) %>%
        show_query()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={k=3, sort_keys=[FieldRef.Name(wt) DESC]}}
        2:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          1:FilterNode{filter=(mpg > 20)}
            0:TableSourceNode{}
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          0:SourceNode{}

---

    Code
      mtcars %>% arrow_table() %>% filter(mpg > 20) %>% arrange(desc(wt)) %>% head(3) %>%
        explain()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={k=3, sort_keys=[FieldRef.Name(wt) DESC]}}
        2:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          1:FilterNode{filter=(mpg > 20)}
            0:TableSourceNode{}
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb]}
          0:SourceNode{}

