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


from itertools import count
from numbers import Integral

from pyarrow import types
from pyarrow.lib import Schema
import pyarrow._orc as _orc


def _is_map(typ):
    return (types.is_list(typ) and
            types.is_struct(typ.value_type) and
            typ.value_type.num_children == 2 and
            typ.value_type[0].name == 'key' and
            typ.value_type[1].name == 'value')


def _traverse(typ, counter):
    if isinstance(typ, Schema) or types.is_struct(typ):
        for field in typ:
            path = (field.name,)
            yield path, next(counter)
            for sub, c in _traverse(field.type, counter):
                yield path + sub, c
    elif _is_map(typ):
        yield from _traverse(typ.value_type, counter)
    elif types.is_list(typ):
        # Skip one index for list type, since this can never be selected
        # directly
        next(counter)
        yield from _traverse(typ.value_type, counter)
    elif types.is_union(typ):
        # Union types not supported, just skip the indexes
        for dtype in typ:
            next(counter)
            for sub_c in _traverse(dtype, counter):
                pass


def _schema_to_indices(schema):
    return {'.'.join(i): c for i, c in _traverse(schema, count(1))}


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
        self._column_index_lookup = _schema_to_indices(self.schema)

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

    def _select_indices(self, columns=None):
        if columns is None:
            return None

        schema = self.schema
        indices = []
        for col in columns:
            if isinstance(col, Integral):
                col = int(col)
                if 0 <= col < len(schema):
                    col = schema[col].name
                else:
                    raise ValueError("Column indices must be in 0 <= ind < %d,"
                                     " got %d" % (len(schema), col))
            if col in self._column_index_lookup:
                indices.append(self._column_index_lookup[col])
            else:
                raise ValueError("Unknown column name %r" % col)

        return indices

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
        include_indices = self._select_indices(columns)
        return self.reader.read_stripe(n, include_indices=include_indices)

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
        include_indices = self._select_indices(columns)
        return self.reader.read(include_indices=include_indices)
