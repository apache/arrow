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

# flake8: noqa

import itertools

import numpy as np

import sys
import six
from six import BytesIO, StringIO, string_types as py_string


PY26 = sys.version_info[:2] == (2, 6)
PY2 = sys.version_info[0] == 2


if PY26:
    import unittest2 as unittest
else:
    import unittest


if PY2:
    import cPickle

    try:
        from cdecimal import Decimal
    except ImportError:
        from decimal import Decimal

    unicode_type = unicode
    lzip = zip
    zip = itertools.izip

    def dict_values(x):
        return x.values()

    range = xrange
    long = long

    def u(s):
        return unicode(s, "unicode_escape")

    def tobytes(o):
        if isinstance(o, unicode):
            return o.encode('utf8')
        else:
            return o

    def frombytes(o):
        return o
else:
    unicode_type = str
    def lzip(*x):
        return list(zip(*x))
    long = int
    zip = zip
    def dict_values(x):
        return list(x.values())
    from decimal import Decimal
    range = range

    def u(s):
        return s

    def tobytes(o):
        if isinstance(o, str):
            return o.encode('utf8')
        else:
            return o

    def frombytes(o):
        return o.decode('utf8')


integer_types = six.integer_types + (np.integer,)
