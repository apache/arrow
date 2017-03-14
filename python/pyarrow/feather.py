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

import six
from distutils.version import LooseVersion
import pandas as pd

from pyarrow.compat import pdapi
from pyarrow._feather import FeatherError  # noqa
from pyarrow.table import Table
import pyarrow._feather as ext


if LooseVersion(pd.__version__) < '0.17.0':
    raise ImportError("feather requires pandas >= 0.17.0")


class FeatherReader(ext.FeatherReader):

    def __init__(self, source):
        self.source = source
        self.open(source)

    def read(self, columns=None):
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
        return table.to_pandas()


def write_feather(df, path):
    '''
    Write a pandas.DataFrame to Feather format
    '''
    writer = ext.FeatherWriter()
    writer.open(path)

    if isinstance(df, pd.SparseDataFrame):
        df = df.to_dense()

    if not df.columns.is_unique:
        raise ValueError("cannot serialize duplicate column names")

    # TODO(wesm): pipeline conversion to Arrow memory layout
    for i, name in enumerate(df.columns):
        col = df.iloc[:, i]

        if pdapi.is_object_dtype(col):
            inferred_type = pd.lib.infer_dtype(col)
            msg = ("cannot serialize column {n} "
                   "named {name} with dtype {dtype}".format(
                       n=i, name=name, dtype=inferred_type))

            if inferred_type in ['mixed']:

                # allow columns with nulls + an inferable type
                inferred_type = pd.lib.infer_dtype(col[col.notnull()])
                if inferred_type in ['mixed']:
                    raise ValueError(msg)

            elif inferred_type not in ['unicode', 'string']:
                raise ValueError(msg)

        if not isinstance(name, six.string_types):
            name = str(name)

        writer.write_array(name, col)

    writer.close()


def read_feather(path, columns=None):
    """
    Read a pandas.DataFrame from Feather format

    Parameters
    ----------
    path : string, path to read from
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read

    Returns
    -------
    df : pandas.DataFrame
    """
    reader = FeatherReader(path)
    return reader.read(columns=columns)
