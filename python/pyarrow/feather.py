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
import warnings

from pyarrow.compat import pdapi
from pyarrow.lib import FeatherError  # noqa
from pyarrow.lib import RecordBatch, Table, concat_tables
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

    def read(self, *args, **kwargs):
        warnings.warn("read has been deprecated. Use read_pandas instead.",
                      DeprecationWarning)
        return self.read_pandas(*args, **kwargs)

    def read_table(self, columns=None):
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
        return table

    def read_pandas(self, columns=None, nthreads=1):
        return self.read_table(columns=columns).to_pandas(nthreads=nthreads)


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

        # TODO(wesm): Remove this length check, see ARROW-1732
        if len(df.columns) > 0:
            batch = RecordBatch.from_pandas(df, preserve_index=False)
            for i, name in enumerate(batch.schema.names):
                col = batch[i]
                self.writer.write_array(name, col)

        self.writer.close()


class FeatherDataset(object):
    """
    Encapsulates details of reading a list of Feather files.

    Parameters
    ----------
    path_or_paths : List[str]
        A list of file names
    validate_schema : boolean, default True
        Check that individual file schemas are all the same / compatible
    """
    def __init__(self, path_or_paths, validate_schema=True):
        self.paths = path_or_paths
        self.validate_schema = validate_schema

    def read_table(self, columns=None):
        """
        Read multiple feather files as a single pyarrow.Table

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns)
        """
        _fil = FeatherReader(self.paths[0]).read_table(columns=columns)
        self._tables = [_fil]
        self.schema = _fil.schema

        for fil in self.paths[1:]:
            fil_table = FeatherReader(fil).read_table(columns=columns)
            if self.validate_schema:
                self.validate_schemas(fil, fil_table)
            self._tables.append(fil_table)
        return concat_tables(self._tables)

    def validate_schemas(self, piece, table):
        if not self.schema.equals(table.schema):
            raise ValueError('Schema in {0!s} was different. \n'
                             '{1!s}\n\nvs\n\n{2!s}'
                             .format(piece, self.schema,
                                     table.schema))

    def read_pandas(self, columns=None, nthreads=1):
        """
        Read multiple Parquet files as a single pandas DataFrame

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        nthreads : int, default 1
            Number of columns to read in parallel.

        Returns
        -------
        pandas.DataFrame
            Content of the file as a pandas DataFrame (of columns)
        """
        return self.read_table(columns=columns).to_pandas(nthreads=nthreads)


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
    return reader.read_pandas(columns=columns, nthreads=nthreads)


def read_table(source, columns=None):
    """
    Read a pyarrow.Table from Feather format

    Parameters
    ----------
    source : string file path, or file-like object
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read

    Returns
    -------
    table : pyarrow.Table
    """
    reader = FeatherReader(source)
    return reader.read_table(columns=columns)
