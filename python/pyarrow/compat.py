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

import sys
import socket
import unittest

import numpy as np

try:
    import pickle5 as builtin_pickle
except ImportError:
    import pickle as builtin_pickle

from collections.abc import Iterable, Mapping, Sequence

def guid():
    from uuid import uuid4
    return uuid4().hex

def tobytes(o):
    if isinstance(o, str):
        return o.encode('utf8')
    else:
        return o

def frombytes(o, *, safe=False):
    if safe:
        return o.decode('utf8', errors='replace')
    else:
        return o.decode('utf8')


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
    if isinstance(path, str):
        # POSIX systems can handle utf-8. UTF8 is converted to utf16-le in
        # libarrow
        encoded_path = path.encode('utf-8')
    else:
        encoded_path = path

    # Windows file system requires utf-16le for file names; Arrow C++ libraries
    # will convert utf8 to utf16
    return encoded_path


integer_types = (int, np.integer,)


def get_socket_from_fd(fileno, family, type):
    return socket.socket(fileno=fileno, family=family, type=type)


try:
    # This function is available after numpy-0.16.0.
    # See also: https://github.com/numpy/numpy/blob/master/numpy/lib/format.py
    from numpy.lib.format import descr_to_dtype
except ImportError:
    def descr_to_dtype(descr):
        '''
        descr may be stored as dtype.descr, which is a list of
        (name, format, [shape]) tuples where format may be a str or a tuple.
        Offsets are not explicitly saved, rather empty fields with
        name, format == '', '|Vn' are added as padding.
        This function reverses the process, eliminating the empty padding fields.
        '''
        if isinstance(descr, str):
            # No padding removal needed
            return np.dtype(descr)
        elif isinstance(descr, tuple):
            # subtype, will always have a shape descr[1]
            dt = descr_to_dtype(descr[0])
            return np.dtype((dt, descr[1]))
        fields = []
        offset = 0
        for field in descr:
            if len(field) == 2:
                name, descr_str = field
                dt = descr_to_dtype(descr_str)
            else:
                name, descr_str, shape = field
                dt = np.dtype((descr_to_dtype(descr_str), shape))

            # Ignore padding bytes, which will be void bytes with '' as name
            # Once support for blank names is removed, only "if name == ''" needed)
            is_pad = (name == '' and dt.type is np.void and dt.names is None)
            if not is_pad:
                fields.append((name, dt, offset))

            offset += dt.itemsize

        names, formats, offsets = zip(*fields)
        # names may be (title, names) tuples
        nametups = (n  if isinstance(n, tuple) else (None, n) for n in names)
        titles, names = zip(*nametups)
        return np.dtype({'names': names, 'formats': formats, 'titles': titles,
                            'offsets': offsets, 'itemsize': offset})

__all__ = []
