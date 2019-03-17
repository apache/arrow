def sum(Array values):
    cdef CDatum out

    with nogil:
        check_status(Sum(_context(), CDatum(values.sp_array), &out))

    return wrap_datum(out)
