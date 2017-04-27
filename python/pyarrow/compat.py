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

from distutils.version import LooseVersion
import itertools

import numpy as np

import sys
import six
from six import BytesIO, StringIO, string_types as py_string


PY26 = sys.version_info[:2] == (2, 6)
PY2 = sys.version_info[0] == 2

try:
    import pandas as pd
    pdver = LooseVersion(pd.__version__)
    if pdver >= '0.20.0':
        try:
            from pandas.api.types import DatetimeTZDtype
        except AttributeError:
            # can be removed once 0.20.0 is released
            from pandas.core.dtypes.dtypes import DatetimeTZDtype

        pdapi = pd.api.types
    elif pdver < '0.19.0':
        from pandas.core.dtypes import DatetimeTZDtype
        pdapi = pd.core.common
    else:
        from pandas.types.dtypes import DatetimeTZDtype
        pdapi = pd.api.types

    PandasSeries = pd.Series
    Categorical = pd.Categorical
    HAVE_PANDAS = True
except:
    HAVE_PANDAS = False
    class DatetimeTZDtype(object):
        pass

    class ClassPlaceholder(object):

        def __init__(self, *args, **kwargs):
            raise NotImplementedError

    class PandasSeries(ClassPlaceholder):
        pass

    class Categorical(ClassPlaceholder):
        pass


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

    def guid():
        from uuid import uuid4
        return uuid4().get_hex()

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

    def guid():
        from uuid import uuid4
        return uuid4().hex

    def u(s):
        return s

    def tobytes(o):
        if isinstance(o, str):
            return o.encode('utf8')
        else:
            return o

    def frombytes(o):
        return o.decode('utf8')


def encode_file_path(path):
    import os
    # Windows requires utf-16le encoding for unicode file names
    if isinstance(path, unicode_type):
        # POSIX systems can handle utf-8. UTF8 is converted to utf16-le in
        # libarrow
        encoded_path = path.encode('utf-8')
    else:
        encoded_path = path

    return encoded_path


integer_types = six.integer_types + (np.integer,)

__all__ = []
