# RecordBatch doesn't support rbind

    Use `Table$create()` to combine RecordBatches into a Table

# RecordBatch supports cbind

    Non-scalar inputs must have an equal number of rows.
    i ..1 has 10, ..2 has 2

# as_record_batch() works for data.frame()

    Error converting columns to Array
    i Problem column: `x`
    Caused by error:
    ! Unrecognized vector instance for type ENVSXP

