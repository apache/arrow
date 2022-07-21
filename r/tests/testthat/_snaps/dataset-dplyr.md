# how_query() and explain() with datasets

    Code
      ds %>% show_query()
    Output
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          0:SourceNode{}

---

    Code
      ds %>% explain()
    Output
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          0:SourceNode{}

---

    Code
      ds %>% select(string = chr, integer = int, part) %>% filter(integer > 6L &
        part == 1) %>% show_query()
    Output
      ExecPlan with 4 nodes:
      3:SinkNode{}
        2:ProjectNode{projection=["string": chr, "integer": int, part]}
          1:FilterNode{filter=((int > 6) and (cast(part, {to_type=double, allow_int_overflow=false, allow_time_truncate=false, allow_time_overflow=false, allow_decimal_truncate=false, allow_float_truncate=false, allow_invalid_utf8=false}) == 1))}
            0:SourceNode{}

---

    Code
      ds %>% select(string = chr, integer = int, part) %>% filter(integer > 6L &
        part == 1) %>% explain()
    Output
      ExecPlan with 4 nodes:
      3:SinkNode{}
        2:ProjectNode{projection=["string": chr, "integer": int, part]}
          1:FilterNode{filter=((int > 6) and (cast(part, {to_type=double, allow_int_overflow=false, allow_time_truncate=false, allow_time_overflow=false, allow_decimal_truncate=false, allow_float_truncate=false, allow_invalid_utf8=false}) == 1))}
            0:SourceNode{}

---

    Code
      ds %>% group_by(part) %>% summarise(avg = mean(int)) %>% show_query()
    Output
      ExecPlan with 6 nodes:
      5:SinkNode{}
        4:ProjectNode{projection=[part, avg]}
          3:ProjectNode{projection=[part, avg]}
            2:GroupByNode{keys=["part"], aggregates=[
            	hash_mean(avg, {skip_nulls=false, min_count=0}),
            ]}
              1:ProjectNode{projection=["avg": int, part]}
                0:SourceNode{}

---

    Code
      ds %>% group_by(part) %>% summarise(avg = mean(int)) %>% explain()
    Output
      ExecPlan with 6 nodes:
      5:SinkNode{}
        4:ProjectNode{projection=[part, avg]}
          3:ProjectNode{projection=[part, avg]}
            2:GroupByNode{keys=["part"], aggregates=[
            	hash_mean(avg, {skip_nulls=false, min_count=0}),
            ]}
              1:ProjectNode{projection=["avg": int, part]}
                0:SourceNode{}

---

    Code
      ds %>% filter(lgl) %>% arrange(chr) %>% show_query()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={sort_keys=[FieldRef.Name(chr) ASC], null_placement=AtEnd}}
        2:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          1:FilterNode{filter=lgl}
            0:SourceNode{}

---

    Code
      ds %>% filter(lgl) %>% arrange(chr) %>% explain()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={sort_keys=[FieldRef.Name(chr) ASC], null_placement=AtEnd}}
        2:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          1:FilterNode{filter=lgl}
            0:SourceNode{}

---

    Code
      ds %>% filter(lgl) %>% arrange(chr) %>% head() %>% show_query()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={k=6, sort_keys=[FieldRef.Name(chr) ASC]}}
        2:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          1:FilterNode{filter=lgl}
            0:SourceNode{}
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          0:SourceNode{}

---

    Code
      ds %>% filter(lgl) %>% arrange(chr) %>% head() %>% explain()
    Output
      ExecPlan with 4 nodes:
      3:OrderBySinkNode{by={k=6, sort_keys=[FieldRef.Name(chr) ASC]}}
        2:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          1:FilterNode{filter=lgl}
            0:SourceNode{}
      ExecPlan with 3 nodes:
      2:SinkNode{}
        1:ProjectNode{projection=[int, dbl, lgl, chr, fct, ts, part]}
          0:SourceNode{}

