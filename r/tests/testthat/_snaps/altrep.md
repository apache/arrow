# R checks for bounds

    Code
      (expect_error(v_int[[5]]))
    Output
      <simpleError in v_int[[5]]: subscript out of bounds>
    Code
      (expect_error(v_dbl[[5]]))
    Output
      <simpleError in v_dbl[[5]]: subscript out of bounds>
    Code
      (expect_error(v_str[[5]]))
    Output
      <simpleError in v_str[[5]]: subscript out of bounds>

