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

from distutils.version import LooseVersion
import os

import six
import pandas as pd

from pyarrow.compat import pdapi
from pyarrow.lib import FeatherError  # noqa
from pyarrow.lib import Table
import pyarrow.lib as ext

try:
    infer_dtype = pdapi.infer_dtype
except AttributeError:
    infer_dtype = pd.lib.infer_dtype


if LooseVersion(pd.__version__) < '0.17.0':
    raise ImportError("feather requires pandas >= 0.17.0")


class FeatherReader(ext.FeatherReader):

    def __init__(self, source):
        self.source = source
        self.open(source)

    def read(self, columns=None, nthreads=1):
        if columns is not None:
            column_set = set(columns)
        else:
            column_set = None

        columns = []
        names = []
        for i in range(self.num_columns):
            name = self.get_column_name(i)
            if column_set is None or name in column_set:
                col = self.get_column(i)
                columns.append(col)
                names.append(name)

        table = Table.from_arrays(columns, names=names)
        return table.to_pandas(nthreads=nthreads)


class FeatherWriter(object):

    def __init__(self, dest):
        self.dest = dest
        self.writer = ext.FeatherWriter()
        self.writer.open(dest)

    def write(self, df):
        if isinstance(df, pd.SparseDataFrame):
            df = df.to_dense()

        if not df.columns.is_unique:
            raise ValueError("cannot serialize duplicate column names")

        # TODO(wesm): pipeline conversion to Arrow memory layout
        for i, name in enumerate(df.columns):
            col = df.iloc[:, i]

            if pdapi.is_object_dtype(col):
                inferred_type = infer_dtype(col)
                msg = ("cannot serialize column {n} "
                       "named {name} with dtype {dtype}".format(
                           n=i, name=name, dtype=inferred_type))

                if inferred_type in ['mixed']:

                    # allow columns with nulls + an inferable type
                    inferred_type = infer_dtype(col[col.notnull()])
                    if inferred_type in ['mixed']:
                        raise ValueError(msg)

                elif inferred_type not in ['unicode', 'string']:
                    raise ValueError(msg)

            if not isinstance(name, six.string_types):
                name = str(name)

            self.writer.write_array(name, col)

        self.writer.close()


def write_feather(df, dest):
    """
    Write a pandas.DataFrame to Feather format

    Parameters
    ----------
    df : pandas.DataFrame
    dest : string
        Local file path
    """
    writer = FeatherWriter(dest)
    try:
        writer.write(df)
    except Exception:
        # Try to make sure the resource is closed
        import gc
        writer = None
        gc.collect()
        if isinstance(dest, six.string_types):
            try:
                os.remove(dest)
            except os.error:
                pass
        raise


def read_feather(source, columns=None, nthreads=1):
    """
    Read a pandas.DataFrame from Feather format

    Parameters
    ----------
    source : string file path, or file-like object
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read
    nthreads : int, default 1
        Number of CPU threads to use when reading to pandas.DataFrame

    Returns
    -------
    df : pandas.DataFrame
    """
    reader = FeatherReader(source)
    return reader.read(columns=columns, nthreads=nthreads)
