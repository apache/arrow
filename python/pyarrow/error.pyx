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

from pyarrow.includes.libarrow cimport CStatus
from pyarrow.includes.common cimport c_string
from pyarrow.compat import frombytes


class ArrowException(ValueError):
    pass


class ArrowMemoryError(MemoryError):
    pass


class ArrowIOError(IOError):
    pass


class ArrowKeyError(KeyError):
    pass


class ArrowNotImplementedError(NotImplementedError):
    pass


cdef int check_status(const CStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        message = frombytes(status.ToString())
        if status.IsIOError():
            raise ArrowIOError(message)
        elif status.IsOutOfMemory():
            raise ArrowMemoryError(message)
        elif status.IsKeyError():
            raise ArrowKeyError(message)
        elif status.IsNotImplemented():
            raise ArrowNotImplementedError(message)
        else:
            raise ArrowException(message)
