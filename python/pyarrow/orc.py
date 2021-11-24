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


from numbers import Integral
import warnings

from pyarrow.lib import Table
import pyarrow._orc as _orc


class ORCFile:
    """
    Reader interface for a single ORC file

    Parameters
    ----------
    source : str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    """

    def __init__(self, source):
        self.reader = _orc.ORCReader()
        self.reader.open(source)

    @property
    def metadata(self):
        """The file metadata, as an arrow KeyValueMetadata"""
        return self.reader.metadata()

    @property
    def schema(self):
        """The file schema, as an arrow schema"""
        return self.reader.schema()

    @property
    def nrows(self):
        """The number of rows in the file"""
        return self.reader.nrows()

    @property
    def nstripes(self):
        """The number of stripes in the file"""
        return self.reader.nstripes()

    def _select_names(self, columns=None):
        if columns is None:
            return None

        schema = self.schema
        names = []
        for col in columns:
            if isinstance(col, Integral):
                col = int(col)
                if 0 <= col < len(schema):
                    col = schema[col].name
                    names.append(col)
                else:
                    raise ValueError("Column indices must be in 0 <= ind < %d,"
                                     " got %d" % (len(schema), col))
            else:
                return columns

        return names

    def read_stripe(self, n, columns=None):
        """Read a single stripe from the file.

        Parameters
        ----------
        n : int
            The stripe index
        columns : list
            If not None, only these columns will be read from the stripe. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'

        Returns
        -------
        pyarrow.lib.RecordBatch
            Content of the stripe as a RecordBatch.
        """
        columns = self._select_names(columns)
        return self.reader.read_stripe(n, columns=columns)

    def read(self, columns=None):
        """Read the whole file.

        Parameters
        ----------
        columns : list
            If not None, only these columns will be read from the file. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'

        Returns
        -------
        pyarrow.lib.Table
            Content of the file as a Table.
        """
        columns = self._select_names(columns)
        return self.reader.read(columns=columns)


class ORCWriter:
    """
    Writer interface for a single ORC file

    Parameters
    ----------
    where : str or pyarrow.io.NativeFile
        Writable target. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface, pyarrow.io.BufferOutputStream
        or pyarrow.io.FixedSizeBufferWriter.
    """

    def __init__(self, where):
        self.writer = _orc.ORCWriter()
        self.writer.open(where)

    def write(self, table):
        """
        Write the table into an ORC file. The schema of the table must
        be equal to the schema used when opening the ORC file.

        Parameters
        ----------
        schema : pyarrow.lib.Table
            The table to be written into the ORC file
        """
        self.writer.write(table)

    def close(self):
        """
        Close the ORC file
        """
        self.writer.close()


def write_table(table, where):
    """
    Write a table into an ORC file

    Parameters
    ----------
    table : pyarrow.lib.Table
        The table to be written into the ORC file
    where : str or pyarrow.io.NativeFile
        Writable target. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface, pyarrow.io.BufferOutputStream
        or pyarrow.io.FixedSizeBufferWriter.
    """
    if isinstance(where, Table):
        warnings.warn(
            "The order of the arguments has changed. Pass as "
            "'write_table(table, where)' instead. The old order will raise "
            "an error in the future.", FutureWarning, stacklevel=2
        )
        table, where = where, table
    writer = ORCWriter(where)
    writer.write(table)
    writer.close()
