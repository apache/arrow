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
import pandas as pd

import six

from pyarrow.compat import PY2


INDEX_LEVEL_NAME_REGEX = re.compile(r'^__index_level_\d+__$')


def is_unnamed_index_level(name):
    return INDEX_LEVEL_NAME_REGEX.match(name) is not None


def infer_dtype(column):
    try:
        return pd.api.types.infer_dtype(column)
    except AttributeError:
        return pd.lib.infer_dtype(column)


def get_column_metadata(column, name):
    inferred_dtype = infer_dtype(column)
    dtype = column.dtype

    if hasattr(dtype, 'categories'):
        extra_metadata = {
            'num_categories': len(column.cat.categories),
            'ordered': column.cat.ordered,
        }
    elif hasattr(dtype, 'tz'):
        extra_metadata = {'timezone': str(dtype.tz)}
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
        'pandas_type': {
            'string': 'bytes' if PY2 else 'unicode',
            'datetime64': (
                'datetimetz' if hasattr(dtype, 'tz')
                else 'datetime'
            ),
            'integer': str(dtype),
            'floating': str(dtype),
        }.get(inferred_dtype, inferred_dtype),
        'numpy_type': str(dtype),
        'metadata': extra_metadata,
    }


def index_level_name(index, i):
    return index.name or '__index_level_{:d}__'.format(i)


def construct_metadata(df, index_levels, preserve_index):
    return {
        b'pandas': json.dumps(
            {
                'index_columns': [
                    index_level_name(level, i)
                    for i, level in enumerate(index_levels)
                ] if preserve_index else [],
                'columns': [
                    get_column_metadata(df[name], name=name)
                    for name in df.columns
                ] + (
                    [
                        get_column_metadata(
                            level, name=index_level_name(level, i)
                        )
                        for i, level in enumerate(index_levels)
                    ] if preserve_index else []
                ),
                'pandas_version': pd.__version__,
            }
        ).encode('utf8')
    }


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
            cat = pd.Categorical(block_arr,
                                 categories=item['dictionary'],
                                 ordered=False, fastpath=True)
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
