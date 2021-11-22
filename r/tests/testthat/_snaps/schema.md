# Schema$code()

    Code
      (expect_error(schema(x = int32(), y = DayTimeInterval__initialize())$code()))
    Output
      <error/rlang_error>
      Error in `code()`:
        Error getting code for field "y"
      Caused by error in `code()`:
        Unsupported type: <day_time_interval>.
        i The arrow package currently has no function to make these.

