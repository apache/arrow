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

# Miscellaneous utility code

import six
import warnings

try:
    # pathlib might not be available
    try:
        import pathlib
    except ImportError:
        import pathlib2 as pathlib  # python 2 backport
    _has_pathlib = True
except ImportError:
    _has_pathlib = False


def implements(f):
    def decorator(g):
        g.__doc__ = f.__doc__
        return g
    return decorator


def _deprecate_api(old_name, new_name, api, next_version):
    msg = ('pyarrow.{0} is deprecated as of {1}, please use {2} instead'
           .format(old_name, next_version, new_name))

    def wrapper(*args, **kwargs):
        warnings.warn(msg, FutureWarning)
        return api(*args)
    return wrapper


def _is_path_like(path):
    # PEP519 filesystem path protocol is available from python 3.6, so pathlib
    # doesn't implement __fspath__ for earlier versions
    return (isinstance(path, six.string_types) or
            hasattr(path, '__fspath__') or
            (_has_pathlib and isinstance(path, pathlib.Path)))


def _stringify_path(path):
    """
    Convert *path* to a string or unicode path if possible.
    """
    if isinstance(path, six.string_types):
        return path

    # checking whether path implements the filesystem protocol
    try:
        return path.__fspath__()  # new in python 3.6
    except AttributeError:
        # fallback pathlib ckeck for earlier python versions than 3.6
        if _has_pathlib and isinstance(path, pathlib.Path):
            return str(path)

    raise TypeError("not a path-like object")


def _deprecate_nthreads(use_threads, nthreads):
    if nthreads is not None:
        warnings.warn("`nthreads` argument is deprecated, "
                      "pass `use_threads` instead", FutureWarning,
                      stacklevel=3)
        if nthreads > 1:
            use_threads = True
    return use_threads
