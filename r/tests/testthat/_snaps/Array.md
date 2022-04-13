# as_arrow_array() default method errors for impossible cases

    Use `concat_arrays()` or `ChunkedArray$create()` instead.
    i `concat_arrays()` creates a new Array by copying data.
    i `ChunkedArray$create()` uses the arrays as chunks for zero-copy concatenation.

---

    Can't create Array<float64()> from object of type class_not_supported

---

    Can't infer Arrow data type from object inheriting from class_not_supported

---

    Can't create Array<float64()> from object of type class_not_supported

# Array doesn't support c()

    Use `concat_arrays()` or `ChunkedArray$create()` instead.
    i `concat_arrays()` creates a new Array by copying data.
    i `ChunkedArray$create()` uses the arrays as chunks for zero-copy concatenation.

