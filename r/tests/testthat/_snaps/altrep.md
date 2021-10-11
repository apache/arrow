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
    Code
      (expect_error(v_int[[-1]]))
    Output
      <simpleError in v_int[[-1]]: invalid negative subscript in get1index <real>>
    Code
      (expect_error(v_dbl[[-1]]))
    Output
      <simpleError in v_dbl[[-1]]: invalid negative subscript in get1index <real>>
    Code
      (expect_error(v_str[[-1]]))
    Output
      <simpleError in v_str[[-1]]: invalid negative subscript in get1index <real>>

