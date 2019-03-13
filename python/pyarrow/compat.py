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
import socket


PY26 = sys.version_info[:2] == (2, 6)
PY2 = sys.version_info[0] == 2


class PandasAPI(object):
    """

    """
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = PandasAPI()
        return cls._instance

    def __init__(self):
        self._imported_pandas = False
        self._have_pandas = False
        self._pd = None
        self._compat_module = None
        self._types_api = None
        self._series = None
        self._categorical_type = None
        self._datetimetz_type = None
        self._get_datetimetz_type = None

    @property
    def make_series(self, *args, **kwargs):
        self._check_import()
        return self._series(*args, **kwargs)

    @property
    def have_pandas(self):
        self._check_import(raise_=False)
        return self._have_pandas

    @property
    def compat(self):
        self._check_import()
        return self._compat_module

    def infer_dtype(self, obj):
        try:
            return self._types_api.infer_dtype(obj, skipna=False)
        except AttributeError:
            return self._pd.lib.infer_dtype(obj)

    @property
    def version(self):
        self._check_import()
        return self._version

    @property
    def categorical_type(self):
        self._check_import()
        return self._categorical_type

    def _check_import(self, raise_=True):
        if self._imported_pandas:
            return

        try:
            import pandas as pd
            import pyarrow.pandas_compat as pdcompat
            self._pd = pd
            self._compat_module = pdcompat
            self._series = pd.Series
            self._have_pandas = True

            self._version = LooseVersion(pd.__version__)
            if self._version >= '0.20.0':
                from pandas.api.types import DatetimeTZDtype
                self._types_api = pd.api.types
            elif pdver >= '0.19.0':
                from pandas.types.dtypes import DatetimeTZDtype
                self._types_api = pd.api.types
            else:
                from pandas.types.dtypes import DatetimeTZDtype
                self._types_api = pd.core.common

            self._datetimetz_type = DatetimeTZDtype
            self._categorical_type = pd.Categorical
        except ImportError:
            if raise_:
                raise

    def is_array_like(self, obj):
        self._check_import()
        return isinstance(obj, (self._pd.Series, self._pd.Index,
                                self._categorical_type))

    def is_categorical(self, obj):
        self._check_import()
        return isinstance(obj, self._categorical_type)

    def is_series(self, obj):
        self._check_import()
        return isinstance(obj, self._series)


if PY26:
    import unittest2 as unittest
else:
    import unittest


if PY2:
    import cPickle as builtin_pickle

    try:
        from cdecimal import Decimal
    except ImportError:
        from decimal import Decimal

    unicode_type = unicode
    file_type = file
    lzip = zip
    zip = itertools.izip
    zip_longest = itertools.izip_longest

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

    def unichar(s):
        return unichr(s)
else:
    try:
        import pickle5 as builtin_pickle
    except ImportError:
        import pickle as builtin_pickle

    unicode_type = str
    file_type = None
    def lzip(*x):
        return list(zip(*x))
    long = int
    zip = zip
    zip_longest = itertools.zip_longest
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

    def unichar(s):
        return chr(s)

try:
    import cloudpickle as pickle
except ImportError:
    pickle = builtin_pickle

def encode_file_path(path):
    import os
    if isinstance(path, unicode_type):
        # POSIX systems can handle utf-8. UTF8 is converted to utf16-le in
        # libarrow
        encoded_path = path.encode('utf-8')
    else:
        encoded_path = path

    # Windows file system requires utf-16le for file names; Arrow C++ libraries
    # will convert utf8 to utf16
    return encoded_path


integer_types = six.integer_types + (np.integer,)


def get_socket_from_fd(fileno, family, type):
    if PY2:
        socket_obj = socket.fromfd(fileno, family, type)
        return socket.socket(family, type, _sock=socket_obj)
    else:
        return socket.socket(fileno=fileno, family=family, type=type)


__all__ = []
