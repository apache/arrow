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

import pathlib
import sys


def _is_path_like(path):
    # PEP519 filesystem path protocol is available from python 3.6, so pathlib
    # doesn't implement __fspath__ for earlier versions
    return (isinstance(path, str) or
            hasattr(path, '__fspath__') or
            isinstance(path, pathlib.Path))


def _ensure_path(path):
    if isinstance(path, pathlib.Path):
        return path
    else:
        return pathlib.Path(_stringify_path(path))


def _stringify_path(path):
    """
    Convert *path* to a string or unicode path if possible.
    """
    if isinstance(path, str):
        return path

    # checking whether path implements the filesystem protocol
    try:
        return path.__fspath__()  # new in python 3.6
    except AttributeError:
        # fallback pathlib ckeck for earlier python versions than 3.6
        if isinstance(path, pathlib.Path):
            return str(path)

    raise TypeError("not a path-like object")


def _import_pandas():
    # ARROW-13425: avoid importing PyArrow from Pandas
    sys.modules['pyarrow'] = None
    import pandas as pd
    return pd
