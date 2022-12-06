# as_arrow_array() works for vctrs_vctr types

    Can't create Array<float64()> from object of type custom_vctr / vctrs_vctr

# as_arrow_array() default method errors

    Can't create Array from object of type class_not_supported

---

    Can't create Array<float64()> from object of type class_not_supported

---

    Can't create Array<float64()> from object of type class_not_supported

---

    Can't create Array<float64()> from object of type class_not_supported

# as_arrow_array() works for blob::blob()

    Can't create Array<int32()> from object of type blob / vctrs_list_of / vctrs_vctr / list

# as_arrow_array() works for vctrs::list_of()

    Can't create Array<int32()> from object of type vctrs_list_of / vctrs_vctr / list

# Array doesn't support c()

    Use `concat_arrays()` or `ChunkedArray$create()` instead.
    i `concat_arrays()` creates a new Array by copying data.
    i `ChunkedArray$create()` uses the arrays as chunks for zero-copy concatenation.

