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

from __future__ import absolute_import

import itertools

import numpy as np

import sys
import six
from six import BytesIO, StringIO, string_types as py_string
import socket


PY26 = sys.version_info[:2] == (2, 6)
PY2 = sys.version_info[0] == 2


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

    from collections import Iterable, Mapping, Sequence

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

    def u_utf8(s):
        return s.decode('utf-8')

    def frombytes(o):
        return o

    def unichar(s):
        return unichr(s)
else:
    try:
        import pickle5 as builtin_pickle
    except ImportError:
        import pickle as builtin_pickle

    from collections.abc import Iterable, Mapping, Sequence

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

    def u_utf8(s):
        if isinstance(s, bytes):
            return frombytes(s)
        return s

    def frombytes(o):
        return o.decode('utf8')

    def unichar(s):
        return chr(s)


if sys.version_info >= (3, 7):
    # Starting with Python 3.7, dicts are guaranteed to be insertion-ordered.
    ordered_dict = dict
else:
    import collections
    ordered_dict = collections.OrderedDict


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
