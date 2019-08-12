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

from pyarrow.includes.libarrow cimport CStatus, IsPyError, RestorePyError
from pyarrow.includes.common cimport c_string
from pyarrow.compat import frombytes


class ArrowException(Exception):
    pass


class ArrowInvalid(ValueError, ArrowException):
    pass


class ArrowMemoryError(MemoryError, ArrowException):
    pass


class ArrowIOError(IOError, ArrowException):
    pass


class ArrowKeyError(KeyError, ArrowException):
    def __str__(self):
        # Override KeyError.__str__, as it uses the repr() of the key
        return ArrowException.__str__(self)


class ArrowTypeError(TypeError, ArrowException):
    pass


class ArrowNotImplementedError(NotImplementedError, ArrowException):
    pass


class ArrowCapacityError(ArrowException):
    pass


class ArrowIndexError(IndexError, ArrowException):
    pass


class ArrowSerializationError(ArrowException):
    pass


# This function could be written directly in C++ if we didn't
# define Arrow-specific subclasses (ArrowInvalid etc.)
cdef int check_status(const CStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        if IsPyError(status):
            RestorePyError(status)
            return -1

        message = frombytes(status.message())
        if status.IsInvalid():
            raise ArrowInvalid(message)
        elif status.IsIOError():
            raise ArrowIOError(message)
        elif status.IsOutOfMemory():
            raise ArrowMemoryError(message)
        elif status.IsKeyError():
            raise ArrowKeyError(message)
        elif status.IsNotImplemented():
            raise ArrowNotImplementedError(message)
        elif status.IsTypeError():
            raise ArrowTypeError(message)
        elif status.IsCapacityError():
            raise ArrowCapacityError(message)
        elif status.IsIndexError():
            raise ArrowIndexError(message)
        elif status.IsSerializationError():
            raise ArrowSerializationError(message)
        else:
            message = frombytes(status.ToString())
            raise ArrowException(message)


# This is an API function for C++ PyArrow
cdef api int pyarrow_internal_check_status(const CStatus& status) \
        nogil except -1:
    return check_status(status)
