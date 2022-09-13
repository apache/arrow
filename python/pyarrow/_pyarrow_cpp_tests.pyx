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

# cython: profile=False
# distutils: language = c++

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status)

from decimal import Decimal

import functools
import inspect

def cytest(func):
    """
    Wraps `func` in a plain Python function.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        bound = inspect.signature(func).bind(*args, **kwargs)
        return func(*bound.args, **bound.kwargs)

    return wrapped

#cdef create_python_decimal(c_string& string_value):
#    cdef PyObject* decimal_value = DecimalFromString(Decimal, string_value)
#    return PyObject_to_object(decimal_value)

@cytest
def test_PythonDecimalToString():
    cdef c_string decimal_string = b'-39402950693754869342983'
    cdef PyObject* decimal_value = DecimalFromString(Decimal, decimal_string)
    cdef c_string string_out

    with nogil:
        check_status(PythonDecimalToString(decimal_value, &string_out))

    assert string_out == decimal_string
