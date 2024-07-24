from libc.stdint cimport *


cdef extern from "<chrono>" namespace "std::chrono":
    cdef cppclass duration:
        duration(int64_t count)
        const int64_t count()

    cdef cppclass nanoseconds(duration):
        nanoseconds(int64_t count)

    T duration_cast[T](duration d)


cdef extern from "<chrono>" namespace "std::chrono::system_clock":
    cdef cppclass time_point:
        time_point(const duration& d)
        const duration time_since_epoch()