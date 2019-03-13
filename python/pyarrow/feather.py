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

import os

import six

from pyarrow.pandas_compat import _pandas_api  # noqa
from pyarrow.lib import FeatherError  # noqa
from pyarrow.lib import Table, concat_tables
import pyarrow.lib as ext


def _check_pandas_version():
    if _pandas_api.loose_version < '0.17.0':
        raise ImportError("feather requires pandas >= 0.17.0")


class FeatherReader(ext.FeatherReader):

    def __init__(self, source):
        _check_pandas_version()
        self.source = source
        self.open(source)

    def read_table(self, columns=None):
        if columns is None:
            return self._read()
        column_types = [type(column) for column in columns]
        if all(map(lambda t: t == int, column_types)):
            return self._read_indices(columns)
        elif all(map(lambda t: t == str, column_types)):
            return self._read_names(columns)

        column_type_names = [t.__name__ for t in column_types]
        raise TypeError("Columns must be indices or names. "
                        "Got columns {} of types {}"
                        .format(columns, column_type_names))

    def read_pandas(self, columns=None, use_threads=True):
        return self.read_table(columns=columns).to_pandas(
            use_threads=use_threads)


def check_chunked_overflow(col):
    if col.data.num_chunks == 1:
        return

    if col.type in (ext.binary(), ext.string()):
        raise ValueError("Column '{0}' exceeds 2GB maximum capacity of "
                         "a Feather binary column. This restriction may be "
                         "lifted in the future".format(col.name))
    else:
        # TODO(wesm): Not sure when else this might be reached
        raise ValueError("Column '{0}' of type {1} was chunked on conversion "
                         "to Arrow and cannot be currently written to "
                         "Feather format".format(col.name, str(col.type)))


class FeatherWriter(object):

    def __init__(self, dest):
        _check_pandas_version()
        self.dest = dest
        self.writer = ext.FeatherWriter()
        self.writer.open(dest)

    def write(self, df):
        if isinstance(df, _pandas_api.pd.SparseDataFrame):
            df = df.to_dense()

        if not df.columns.is_unique:
            raise ValueError("cannot serialize duplicate column names")

        # TODO(wesm): Remove this length check, see ARROW-1732
        if len(df.columns) > 0:
            table = Table.from_pandas(df, preserve_index=False)
            for i, name in enumerate(table.schema.names):
                col = table[i]
                check_chunked_overflow(col)
                self.writer.write_array(name, col.data.chunk(0))

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
        _check_pandas_version()
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

    def read_pandas(self, columns=None, use_threads=True):
        """
        Read multiple Parquet files as a single pandas DataFrame

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        use_threads : boolean, default True
            Use multiple threads when converting to pandas

        Returns
        -------
        pandas.DataFrame
            Content of the file as a pandas DataFrame (of columns)
        """
        return self.read_table(columns=columns).to_pandas(
            use_threads=use_threads)


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


def read_feather(source, columns=None, use_threads=True):
    """
    Read a pandas.DataFrame from Feather format

    Parameters
    ----------
    source : string file path, or file-like object
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read
    use_threads: bool, default True
        Whether to parallelize reading using multiple threads

    Returns
    -------
    df : pandas.DataFrame
    """
    reader = FeatherReader(source)
    return reader.read_pandas(columns=columns, use_threads=use_threads)


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
