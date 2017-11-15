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

import ast
import collections
import json
import re

import numpy as np
import pandas as pd

import six

import pyarrow as pa
from pyarrow.compat import PY2, zip_longest  # noqa


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


_numpy_logical_type_map = {
    np.bool_: 'bool',
    np.int8: 'int8',
    np.int16: 'int16',
    np.int32: 'int32',
    np.int64: 'int64',
    np.uint8: 'uint8',
    np.uint16: 'uint16',
    np.uint32: 'uint32',
    np.uint64: 'uint64',
    np.float32: 'float32',
    np.float64: 'float64',
    'datetime64[D]': 'date',
    np.str_: 'unicode',
    np.bytes_: 'bytes',
}


def get_logical_type_from_numpy(pandas_collection):
    try:
        return _numpy_logical_type_map[pandas_collection.dtype.type]
    except KeyError:
        if hasattr(pandas_collection.dtype, 'tz'):
            return 'datetimetz'
        return infer_dtype(pandas_collection)


def get_extension_dtype_info(column):
    dtype = column.dtype
    if str(dtype) == 'category':
        cats = getattr(column, 'cat', column)
        assert cats is not None
        metadata = {
            'num_categories': len(cats.categories),
            'ordered': cats.ordered,
        }
        physical_dtype = 'object'
    elif hasattr(dtype, 'tz'):
        metadata = {'timezone': str(dtype.tz)}
        physical_dtype = 'datetime64[ns]'
    else:
        metadata = None
        physical_dtype = str(dtype)
    return physical_dtype, metadata


def get_column_metadata(column, name, arrow_type):
    """Construct the metadata for a given column

    Parameters
    ----------
    column : pandas.Series or pandas.Index
    name : str
    arrow_type : pyarrow.DataType

    Returns
    -------
    dict
    """
    logical_type = get_logical_type(arrow_type)

    string_dtype, extra_metadata = get_extension_dtype_info(column)
    if logical_type == 'decimal':
        extra_metadata = {
            'precision': arrow_type.precision,
            'scale': arrow_type.scale,
        }
        string_dtype = 'object'

    if name is not None and not isinstance(name, six.string_types):
        raise TypeError(
            'Column name must be a string. Got column {} of type {}'.format(
                name, type(name).__name__
            )
        )

    return {
        'name': name,
        'pandas_type': logical_type,
        'numpy_type': string_dtype,
        'metadata': extra_metadata,
    }


index_level_name = '__index_level_{:d}__'.format


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
    df_types = types[:ncolumns - len(index_levels)]
    index_types = types[ncolumns - len(index_levels):]

    column_metadata = [
        get_column_metadata(df[col_name], name=sanitized_name,
                            arrow_type=arrow_type)
        for col_name, sanitized_name, arrow_type in
        zip(df.columns, column_names, df_types)
    ]

    if preserve_index:
        index_column_names = list(map(
            index_level_name, range(len(index_levels))
        ))
        index_column_metadata = [
            get_column_metadata(level, name=level.name, arrow_type=arrow_type)
            for i, (level, arrow_type) in enumerate(
                zip(index_levels, index_types)
            )
        ]

        column_indexes = []

        for level in getattr(df.columns, 'levels', [df.columns]):
            string_dtype, extra_metadata = get_extension_dtype_info(level)
            column_index = {
                'name': level.name,
                'pandas_type': get_logical_type_from_numpy(level),
                'numpy_type': string_dtype,
                'metadata': extra_metadata,
            }
            column_indexes.append(column_index)
    else:
        index_column_names = index_column_metadata = column_indexes = []

    return {
        b'pandas': json.dumps({
            'index_columns': index_column_names,
            'column_indexes': column_indexes,
            'columns': column_metadata + index_column_metadata,
            'pandas_version': pd.__version__
        }).encode('utf8')
    }


def _column_name_to_strings(name):
    """Convert a column name (or level) to either a string or a recursive
    collection of strings.

    Parameters
    ----------
    name : str or tuple

    Returns
    -------
    value : str or tuple

    Examples
    --------
    >>> name = 'foo'
    >>> _column_name_to_strings(name)
    'foo'
    >>> name = ('foo', 'bar')
    >>> _column_name_to_strings(name)
    ('foo', 'bar')
    >>> import pandas as pd
    >>> name = (1, pd.Timestamp('2017-02-01 00:00:00'))
    >>> _column_name_to_strings(name)
    ('1', '2017-02-01 00:00:00')
    """
    if isinstance(name, six.string_types):
        return name
    elif isinstance(name, tuple):
        return tuple(map(_column_name_to_strings, name))
    elif isinstance(name, collections.Sequence):
        raise TypeError("Unsupported type for MultiIndex level")
    elif name is None:
        return None
    return str(name)


def dataframe_to_arrays(df, schema, preserve_index, nthreads=1):
    names = []
    index_columns = []
    type = None

    if preserve_index:
        n = len(getattr(df.index, 'levels', [df.index]))
        index_columns.extend(df.index.get_level_values(i) for i in range(n))

    columns_to_convert = []
    convert_types = []
    for name in df.columns:
        col = df[name]
        if not isinstance(name, six.string_types):
            name = _column_name_to_strings(name)
            if name is not None:
                name = str(name)

        if schema is not None:
            field = schema.field_by_name(name)
            type = getattr(field, "type", None)

        columns_to_convert.append(col)
        convert_types.append(type)
        names.append(name)

    for i, column in enumerate(index_columns):
        columns_to_convert.append(column)
        convert_types.append(None)
        names.append(index_level_name(i))

    # NOTE(wesm): If nthreads=None, then we use a heuristic to decide whether
    # using a thread pool is worth it. Currently the heuristic is whether the
    # nrows > 100 * ncols.
    if nthreads is None:
        nrows, ncols = len(df), len(df.columns)
        if nrows > ncols * 100:
            nthreads = pa.cpu_count()
        else:
            nthreads = 1

    def convert_column(col, ty):
        return pa.array(col, from_pandas=True, type=ty)

    if nthreads == 1:
        arrays = [convert_column(c, t)
                  for c, t in zip(columns_to_convert,
                                  convert_types)]
    else:
        from concurrent import futures
        with futures.ThreadPoolExecutor(nthreads) as executor:
            arrays = list(executor.map(convert_column,
                                       columns_to_convert,
                                       convert_types))

    types = [x.type for x in arrays]

    metadata = construct_metadata(
        df, names, index_columns, preserve_index, types
    )
    return names, arrays, metadata


def get_datetimetz_type(values, dtype, type_):
    from pyarrow.compat import DatetimeTZDtype

    if values.dtype.type != np.datetime64:
        return values, type_

    if isinstance(dtype, DatetimeTZDtype):
        tz = dtype.tz
        unit = dtype.unit
        type_ = pa.timestamp(unit, tz)
    elif type_ is None:
        # Trust the NumPy dtype
        type_ = pa.from_numpy_dtype(values.dtype)

    return values, type_


def make_datetimetz(tz):
    from pyarrow.compat import DatetimeTZDtype
    return DatetimeTZDtype('ns', tz=tz)


def backwards_compatible_index_name(raw_name, logical_name):
    pattern = r'^__index_level_\d+__$'
    if raw_name == logical_name and re.match(pattern, raw_name) is not None:
        return None
    else:
        return logical_name


def table_to_blockmanager(options, table, memory_pool, nthreads=1,
                          categoricals=None):
    import pandas.core.internals as _int
    import pyarrow.lib as lib

    index_columns = []
    columns = []
    column_indexes = []
    index_arrays = []
    index_names = []
    schema = table.schema
    row_count = table.num_rows
    metadata = schema.metadata
    columns_metadata = None

    has_pandas_metadata = metadata is not None and b'pandas' in metadata

    if has_pandas_metadata:
        pandas_metadata = json.loads(metadata[b'pandas'].decode('utf8'))
        index_columns = pandas_metadata['index_columns']
        columns = pandas_metadata['columns']
        column_indexes = pandas_metadata.get('column_indexes', [])
        table = _add_any_metadata(table, pandas_metadata)
        columns_metadata = pandas_metadata.get('columns', None)

    block_table = table

    # Build up a list of index columns and names while removing those columns
    # from the original table
    logical_index_names = [c['name'] for c in columns[-len(index_columns):]]
    for raw_name, logical_name in zip(index_columns, logical_index_names):
        i = schema.get_field_index(raw_name)
        if i != -1:
            col = table.column(i)
            col_pandas = col.to_pandas()
            values = col_pandas.values
            if hasattr(values, 'flags') and not values.flags.writeable:
                # ARROW-1054: in pandas 0.19.2, factorize will reject
                # non-writeable arrays when calling MultiIndex.from_arrays
                values = values.copy()

            index_arrays.append(pd.Series(values, dtype=col_pandas.dtype))
            index_names.append(
                backwards_compatible_index_name(raw_name, logical_name)
            )
            block_table = block_table.remove_column(
                block_table.schema.get_field_index(raw_name)
            )

    # Convert an arrow table to Block from the internal pandas API
    result = lib.table_to_blocks(options, block_table, nthreads, memory_pool)

    # Construct the individual blocks converting dictionary types to pandas
    # categorical types and Timestamps-with-timezones types to the proper
    # pandas Blocks
    blocks = []
    for item in result:
        block_arr = item['block']
        placement = item['placement']
        if 'dictionary' in item:
            cat = pd.Categorical(block_arr,
                                 categories=item['dictionary'],
                                 ordered=item['ordered'], fastpath=True)
            block = _int.make_block(cat, placement=placement,
                                    klass=_int.CategoricalBlock,
                                    fastpath=True)
        elif 'timezone' in item:
            dtype = make_datetimetz(item['timezone'])
            block = _int.make_block(block_arr, placement=placement,
                                    klass=_int.DatetimeTZBlock,
                                    dtype=dtype, fastpath=True)
        else:
            block = _int.make_block(block_arr, placement=placement)
        blocks.append(block)

    # Construct the row index
    if len(index_arrays) > 1:
        index = pd.MultiIndex.from_arrays(index_arrays, names=index_names)
    elif len(index_arrays) == 1:
        index = pd.Index(index_arrays[0], name=index_names[0])
    else:
        index = pd.RangeIndex(row_count)

    column_strings = [x.name for x in block_table.itercolumns()]
    if columns_metadata is not None:
        columns_name_dict = dict(
            (str(x['name']), x['name'])
            for x in columns_metadata
        )
        columns_values = [
            columns_name_dict[y]
            if y in columns_name_dict.keys() else y
            for y in column_strings
        ]
    else:
        columns_values = column_strings

    # If we're passed multiple column indexes then evaluate with
    # ast.literal_eval, since the column index values show up as a list of
    # tuples
    to_pair = ast.literal_eval if len(column_indexes) > 1 else lambda x: (x,)

    # Create the column index

    # Construct the base index
    if not columns_values:
        columns = pd.Index(columns_values)
    else:
        columns = pd.MultiIndex.from_tuples(
            list(map(to_pair, columns_values)),
            names=[col_index['name'] for col_index in column_indexes] or None,
        )

    # if we're reconstructing the index
    if has_pandas_metadata:

        # Get levels and labels, and provide sane defaults if the index has a
        # single level to avoid if/else spaghetti.
        levels = getattr(columns, 'levels', None) or [columns]
        labels = getattr(columns, 'labels', None) or [
            pd.RangeIndex(len(level)) for level in levels
        ]

        # Convert each level to the dtype provided in the metadata
        levels_dtypes = [
            (level, col_index.get('numpy_type', level.dtype))
            for level, col_index in zip_longest(
                levels, column_indexes, fillvalue={}
            )
        ]
        new_levels = [
            _level if _level.dtype == _dtype else _level.astype(_dtype)
            for _level, _dtype in levels_dtypes
        ]

        columns = pd.MultiIndex(
            levels=new_levels,
            labels=labels,
            names=columns.names
        )

    # ARROW-1751: flatten a single level column MultiIndex for pandas 0.21.0
    columns = _flatten_single_level_multiindex(columns)

    axes = [columns, index]
    return _int.BlockManager(blocks, axes)


def _flatten_single_level_multiindex(index):
    if isinstance(index, pd.MultiIndex) and index.nlevels == 1:
        levels, = index.levels
        labels, = index.labels

        # Cheaply check that we do not somehow have duplicate column names
        if not index.is_unique:
            raise ValueError('Found non-unique column index')

        return pd.Index([levels[_label] if _label != -1 else None
                         for _label in labels],
                        name=index.names[0])
    return index


def _add_any_metadata(table, pandas_metadata):
    modified_columns = {}

    schema = table.schema

    # Add time zones
    for i, col_meta in enumerate(pandas_metadata['columns']):
        if col_meta['pandas_type'] == 'datetimetz':
            col = table[i]
            converted = col.to_pandas()
            tz = col_meta['metadata']['timezone']
            tz_aware_type = pa.timestamp('ns', tz=tz)
            with_metadata = pa.Array.from_pandas(converted.values,
                                                 type=tz_aware_type)

            field = pa.field(schema[i].name, tz_aware_type)
            modified_columns[i] = pa.Column.from_array(field,
                                                       with_metadata)

    if len(modified_columns) > 0:
        columns = []
        for i in range(len(table.schema)):
            if i in modified_columns:
                columns.append(modified_columns[i])
            else:
                columns.append(table[i])
        return pa.Table.from_arrays(columns)
    else:
        return table
