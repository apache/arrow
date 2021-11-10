# Writing a CSV errors when unsupported (yet) readr args are used

    Code
      (expect_error(write_csv_arrow(tbl, csv_file, append = FALSE)))
    Output
      <simpleError: The following argument is not yet supported in Arrow: "append">

