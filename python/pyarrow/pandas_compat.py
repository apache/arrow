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

import re
import json
import numpy as np
import pandas as pd

import six

import pyarrow as pa
from pyarrow.compat import PY2  # noqa


INDEX_LEVEL_NAME_REGEX = re.compile(r'^__index_level_\d+__$')


def is_unnamed_index_level(name):
    return INDEX_LEVEL_NAME_REGEX.match(name) is not None


def infer_dtype(column):
    try:
        return pd.api.types.infer_dtype(column)
    except AttributeError:
        return pd.lib.infer_dtype(column)


_logical_type_map = {}


def get_logical_type_map():
    global _logical_type_map

    if not _logical_type_map:
        _logical_type_map.update({
            pa.lib.Type_NA: 'float64',  # NaNs
            pa.lib.Type_BOOL: 'bool',
            pa.lib.Type_INT8: 'int8',
            pa.lib.Type_INT16: 'int16',
            pa.lib.Type_INT32: 'int32',
            pa.lib.Type_INT64: 'int64',
            pa.lib.Type_UINT8: 'uint8',
            pa.lib.Type_UINT16: 'uint16',
            pa.lib.Type_UINT32: 'uint32',
            pa.lib.Type_UINT64: 'uint64',
            pa.lib.Type_HALF_FLOAT: 'float16',
            pa.lib.Type_FLOAT: 'float32',
            pa.lib.Type_DOUBLE: 'float64',
            pa.lib.Type_DATE32: 'date',
            pa.lib.Type_DATE64: 'date',
            pa.lib.Type_TIME32: 'time',
            pa.lib.Type_TIME64: 'time',
            pa.lib.Type_BINARY: 'bytes',
            pa.lib.Type_FIXED_SIZE_BINARY: 'bytes',
            pa.lib.Type_STRING: 'unicode',
        })
    return _logical_type_map


def get_logical_type(arrow_type):
    logical_type_map = get_logical_type_map()

    try:
        return logical_type_map[arrow_type.id]
    except KeyError:
        if isinstance(arrow_type, pa.lib.DictionaryType):
            return 'categorical'
        elif isinstance(arrow_type, pa.lib.ListType):
            return 'list[{}]'.format(get_logical_type(arrow_type.value_type))
        elif isinstance(arrow_type, pa.lib.TimestampType):
            return 'datetimetz' if arrow_type.tz is not None else 'datetime'
        elif isinstance(arrow_type, pa.lib.DecimalType):
            return 'decimal'
        raise NotImplementedError(str(arrow_type))


def get_column_metadata(column, name, arrow_type):
    """Construct the metadata for a given column

    Parameters
    ----------
    column : pandas.Series
    name : str
    arrow_type : pyarrow.DataType

    Returns
    -------
    dict
    """
    dtype = column.dtype
    logical_type = get_logical_type(arrow_type)

    if hasattr(dtype, 'categories'):
        assert logical_type == 'categorical'
        extra_metadata = {
            'num_categories': len(column.cat.categories),
            'ordered': column.cat.ordered,
        }
    elif hasattr(dtype, 'tz'):
        assert logical_type == 'datetimetz'
        extra_metadata = {'timezone': str(dtype.tz)}
    elif logical_type == 'decimal':
        extra_metadata = {
            'precision': arrow_type.precision,
            'scale': arrow_type.scale,
        }
    else:
        extra_metadata = None

    if not isinstance(name, six.string_types):
        raise TypeError(
            'Column name must be a string. Got column {} of type {}'.format(
                name, type(name).__name__
            )
        )

    return {
        'name': name,
        'pandas_type': logical_type,
        'numpy_type': str(dtype),
        'metadata': extra_metadata,
    }


def index_level_name(index, i):
    """Return the name of an index level or a default name if `index.name` is
    None.

    Parameters
    ----------
    index : pandas.Index
    i : int

    Returns
    -------
    name : str
    """
    if index.name is not None:
        return index.name
    else:
        return '__index_level_{:d}__'.format(i)


def construct_metadata(df, column_names, index_levels, preserve_index, types):
    """Returns a dictionary containing enough metadata to reconstruct a pandas
    DataFrame as an Arrow Table, including index columns.

    Parameters
    ----------
    df : pandas.DataFrame
    index_levels : List[pd.Index]
    presere_index : bool
    types : List[pyarrow.DataType]

    Returns
    -------
    dict
    """
    ncolumns = len(column_names)
    df_types = types[:ncolumns]
    index_types = types[ncolumns:ncolumns + len(index_levels)]

    column_metadata = [
        get_column_metadata(df[col_name], name=sanitized_name,
                            arrow_type=arrow_type)
        for col_name, sanitized_name, arrow_type in
        zip(df.columns, column_names, df_types)
    ]

    if preserve_index:
        index_column_names = [index_level_name(level, i)
                              for i, level in enumerate(index_levels)]
        index_column_metadata = [
            get_column_metadata(level, name=index_level_name(level, i),
                                arrow_type=arrow_type)
            for i, (level, arrow_type) in enumerate(zip(index_levels,
                                                        index_types))
        ]
    else:
        index_column_names = index_column_metadata = []

    return {
        b'pandas': json.dumps({
            'index_columns': index_column_names,
            'columns': column_metadata + index_column_metadata,
            'pandas_version': pd.__version__
        }).encode('utf8')
    }


def dataframe_to_arrays(df, timestamps_to_ms, schema, preserve_index):
    names = []
    arrays = []
    index_columns = []
    types = []
    type = None

    if preserve_index:
        n = len(getattr(df.index, 'levels', [df.index]))
        index_columns.extend(df.index.get_level_values(i) for i in range(n))

    for name in df.columns:
        col = df[name]
        if not isinstance(name, six.string_types):
            name = str(name)

        if schema is not None:
            field = schema.field_by_name(name)
            type = getattr(field, "type", None)

        array = pa.Array.from_pandas(
            col, type=type, timestamps_to_ms=timestamps_to_ms
        )
        arrays.append(array)
        names.append(name)
        types.append(array.type)

    for i, column in enumerate(index_columns):
        array = pa.Array.from_pandas(column, timestamps_to_ms=timestamps_to_ms)
        arrays.append(array)
        names.append(index_level_name(column, i))
        types.append(array.type)

    metadata = construct_metadata(
        df, names, index_columns, preserve_index, types
    )
    return names, arrays, metadata


def maybe_coerce_datetime64(values, dtype, type_, timestamps_to_ms=False):
    from pyarrow.compat import DatetimeTZDtype

    if values.dtype.type != np.datetime64:
        return values, type_

    coerce_ms = timestamps_to_ms and values.dtype != 'datetime64[ms]'

    if coerce_ms:
        values = values.astype('datetime64[ms]')
        type_ = pa.timestamp('ms')

    if isinstance(dtype, DatetimeTZDtype):
        tz = dtype.tz
        unit = 'ms' if coerce_ms else dtype.unit
        type_ = pa.timestamp(unit, tz)
    elif type_ is None:
        # Trust the NumPy dtype
        type_ = pa.from_numpy_dtype(values.dtype)

    return values, type_


def table_to_blockmanager(table, nthreads=1):
    import pandas.core.internals as _int
    from pyarrow.compat import DatetimeTZDtype
    import pyarrow.lib as lib

    block_table = table

    index_columns = []
    index_arrays = []
    index_names = []
    schema = table.schema
    row_count = table.num_rows
    metadata = schema.metadata

    if metadata is not None and b'pandas' in metadata:
        pandas_metadata = json.loads(metadata[b'pandas'].decode('utf8'))
        index_columns = pandas_metadata['index_columns']

    for name in index_columns:
        i = schema.get_field_index(name)
        if i != -1:
            col = table.column(i)
            index_name = (None if is_unnamed_index_level(name)
                          else name)
            values = col.to_pandas().values
            if not values.flags.writeable:
                # ARROW-1054: in pandas 0.19.2, factorize will reject
                # non-writeable arrays when calling MultiIndex.from_arrays
                values = values.copy()

            index_arrays.append(values)
            index_names.append(index_name)
            block_table = block_table.remove_column(
                block_table.schema.get_field_index(name)
            )

    result = lib.table_to_blocks(block_table, nthreads)

    blocks = []
    for item in result:
        block_arr = item['block']
        placement = item['placement']
        if 'dictionary' in item:
            ordered = block_table.schema[placement[0]].type.ordered
            cat = pd.Categorical(block_arr,
                                 categories=item['dictionary'],
                                 ordered=ordered, fastpath=True)
            block = _int.make_block(cat, placement=placement,
                                    klass=_int.CategoricalBlock,
                                    fastpath=True)
        elif 'timezone' in item:
            dtype = DatetimeTZDtype('ns', tz=item['timezone'])
            block = _int.make_block(block_arr, placement=placement,
                                    klass=_int.DatetimeTZBlock,
                                    dtype=dtype, fastpath=True)
        else:
            block = _int.make_block(block_arr, placement=placement)
        blocks.append(block)

    if len(index_arrays) > 1:
        index = pd.MultiIndex.from_arrays(index_arrays, names=index_names)
    elif len(index_arrays) == 1:
        index = pd.Index(index_arrays[0], name=index_names[0])
    else:
        index = pd.RangeIndex(row_count)

    axes = [
        [column.name for column in block_table.itercolumns()],
        index
    ]

    return _int.BlockManager(blocks, axes)
