# glimpse() Table/ChunkedArray

    Code
      glimpse(tab)
    Output
      Table
      10 rows x 7 columns
      $ int           <int32> 1, 2, 3, NA, 5, 6, 7, 8, 9, 10
      $ dbl          <double> 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, NA, 10.1
      $ dbl2         <double> 5, 5, 5, 5, 5, 5, 5, 5, 5, 5
      $ lgl            <bool> TRUE, NA, TRUE, FALSE, TRUE, NA, NA, FALSE, FALSE, NA
      $ false          <bool> FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, ~
      $ chr          <string> "a", "b", "c", "d", "e", NA, "g", "h", "i", "j"
      $ fct <dictionary<...>> a, b, c, d, NA, NA, g, h, i, j
      Call `print()` for full schema details

---

    Code
      glimpse(tab$chr)
    Output
      <string> [ [ "a", "b", "c", "d", "e", null, "g", "h", "i", "j" ] ]

# glimpse() RecordBatch/Array

    Code
      glimpse(batch)
    Output
      RecordBatch
      10 rows x 7 columns
      $ int           <int32> 1, 2, 3, NA, 5, 6, 7, 8, 9, 10
      $ dbl          <double> 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, NA, 10.1
      $ dbl2         <double> 5, 5, 5, 5, 5, 5, 5, 5, 5, 5
      $ lgl            <bool> TRUE, NA, TRUE, FALSE, TRUE, NA, NA, FALSE, FALSE, NA
      $ false          <bool> FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, ~
      $ chr          <string> "a", "b", "c", "d", "e", NA, "g", "h", "i", "j"
      $ fct <dictionary<...>> a, b, c, d, NA, NA, g, h, i, j
      Call `print()` for full schema details

---

    Code
      glimpse(batch$int)
    Output
      <int32> [ 1, 2, 3, null, 5, 6, 7, 8, 9, 10 ]

# glimpse() with VctrsExtensionType

    Code
      glimpse(haven)
    Output
      Table
      2 rows x 3 columns
      $ num            <double> 5.1, 4.9
      $ cat_int <ext<hvn_lbll>> 3, 1
      $ cat_chr <ext<hvn_lbll>> Can't convert `x` <haven_labelled> to <character>.
      Call `print()` for full schema details

---

    Code
      glimpse(haven[[3]])
    Output
      <<haven_labelled[0]>> [ [ "B", "B" ] ]

# glimpse prints message about schema if there are complex types

    Code
      glimpse(dictionary_but_no_metadata)
    Output
      Table
      5 rows x 2 columns
      $ a           <int32> 1, 2, 3, 4, 5
      $ b <dictionary<...>> 1, 2, 3, 4, 5
      Call `print()` for full schema details

---

    Code
      glimpse(Table$create(a = 1))
    Output
      Table
      1 rows x 1 columns
      $ a <double> 1

# glimpse() calls print() instead of showing data for RBR

    Code
      example_data %>% as_record_batch_reader() %>% glimpse()
    Message
      Cannot glimpse() data from a RecordBatchReader because it can only be read one time; call `as_arrow_table()` to consume it first.
    Output
      RecordBatchReader
      int: int32
      dbl: double
      dbl2: double
      lgl: bool
      false: bool
      chr: string
      fct: dictionary<values=string, indices=int8>

---

    Code
      example_data %>% as_record_batch_reader() %>% select(int) %>% glimpse()
    Message
      Cannot glimpse() data from a RecordBatchReader because it can only be read one time. Call `compute()` to evaluate the query first.
    Output
      RecordBatchReader (query)
      int: int32
      
      See $.data for the source Arrow object

# glimpse() on Dataset

    Code
      glimpse(ds)
    Output
      FileSystemDataset with 2 Parquet files
      20 rows x 7 columns
      $ int                <int32> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 101, 102, 103, 104, ~
      $ dbl               <double> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 51, 52, 53, 54, 55, ~
      $ lgl                 <bool> TRUE, FALSE, NA, TRUE, FALSE, TRUE, FALSE, NA, TRUE~
      $ chr               <string> "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "~
      $ fct      <dictionary<...>> A, B, C, D, E, F, G, H, I, J, J, I, H, G, F, E, D, ~
      $ ts <timestamp[us, tz=UTC]> 2015-04-30 03:12:39, 2015-05-01 03:12:39, 2015-05-0~
      $ group              <int32> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, ~
      Call `print()` for full schema details

# glimpse() on Dataset query only shows data for streaming eval

    Code
      ds %>% summarize(max(int)) %>% glimpse()
    Message
      This query requires a full table scan, so glimpse() may be expensive. Call `compute()` to evaluate the query first.
    Output
      FileSystemDataset (query)
      max(int): int32
      
      See $.data for the source Arrow object

# glimpse() on in-memory query shows data even if aggregating

    Code
      example_data %>% arrow_table() %>% summarize(sum(int, na.rm = TRUE)) %>%
        glimpse()
    Output
      Table (query)
      ?? rows x 1 columns
      $ `sum(int, na.rm = TRUE)` <int64> 51
      Call `print()` for query details

