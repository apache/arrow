# Writing a CSV errors when unsupported (yet) readr args are used

    Code
      write_csv_arrow(tbl, csv_file, append = FALSE)
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * append = FALSE

---

    Code
      write_csv_arrow(tbl, csv_file, quote = "all")
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * quote = "all"

---

    Code
      write_csv_arrow(tbl, csv_file, escape = "double")
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * escape = "double"

---

    Code
      write_csv_arrow(tbl, csv_file, eol = "\n")
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * eol = "\n"

---

    Code
      write_csv_arrow(tbl, csv_file, num_threads = 8)
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * num_threads = 8

---

    Code
      write_csv_arrow(tbl, csv_file, progress = FALSE)
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic argument:
      * progress = FALSE

---

    Code
      write_csv_arrow(tbl, csv_file, append = FALSE, eol = "\n")
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic arguments:
      * append = FALSE
      * eol = "\n"

---

    Code
      write_csv_arrow(tbl, csv_file, append = FALSE, quote = "all", escape = "double",
        eol = "\n")
    Condition
      Error in `write_csv_arrow()`:
      ! Arguments not yet supported in Arrow
      * `...` must be empty.
      x Problematic arguments:
      * append = FALSE
      * quote = "all"
      * escape = "double"
      * eol = "\n"

