# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


cdef class CastOptions:

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, allow_int_overflow=None,
                 allow_time_truncate=None, allow_time_overflow=None,
                 allow_float_truncate=None, allow_invalid_utf8=None):
        if allow_int_overflow is not None:
            self.allow_int_overflow = allow_int_overflow
        if allow_time_truncate is not None:
            self.allow_time_truncate = allow_time_truncate
        if allow_time_overflow is not None:
            self.allow_time_overflow = allow_time_overflow
        if allow_float_truncate is not None:
            self.allow_float_truncate = allow_float_truncate
        if allow_invalid_utf8 is not None:
            self.allow_invalid_utf8 = allow_invalid_utf8

    @staticmethod
    cdef wrap(CCastOptions options):
        cdef CastOptions self = CastOptions.__new__(CastOptions)
        self.options = options
        return self

    @staticmethod
    def safe():
        return CastOptions.wrap(CCastOptions.Safe())

    @staticmethod
    def unsafe():
        return CastOptions.wrap(CCastOptions.Unsafe())

    cdef inline CCastOptions unwrap(self) nogil:
        return self.options

    @property
    def allow_int_overflow(self):
        return self.options.allow_int_overflow

    @allow_int_overflow.setter
    def allow_int_overflow(self, bint flag):
        self.options.allow_int_overflow = flag

    @property
    def allow_time_truncate(self):
        return self.options.allow_time_truncate

    @allow_time_truncate.setter
    def allow_time_truncate(self, bint flag):
        self.options.allow_time_truncate = flag

    @property
    def allow_time_overflow(self):
        return self.options.allow_time_overflow

    @allow_time_overflow.setter
    def allow_time_overflow(self, bint flag):
        self.options.allow_time_overflow = flag

    @property
    def allow_float_truncate(self):
        return self.options.allow_float_truncate

    @allow_float_truncate.setter
    def allow_float_truncate(self, bint flag):
        self.options.allow_float_truncate = flag

    @property
    def allow_invalid_utf8(self):
        return self.options.allow_invalid_utf8

    @allow_invalid_utf8.setter
    def allow_invalid_utf8(self, bint flag):
        self.options.allow_invalid_utf8 = flag
