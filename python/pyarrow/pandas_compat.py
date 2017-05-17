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
