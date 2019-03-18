def sum(Array values):
    """
    Sum the values in a numerical array.

    Parameters
    ----------
    values : numerical pyarrow.Array

    Returns
    -------
    scalar : Scalar representing the sum of values.
    """
    cdef CDatum out

    with nogil:
        check_status(Sum(_context(), CDatum(values.sp_array), &out))

    return wrap_datum(out)
