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

from collections import OrderedDict, defaultdict
import six
import sys

import numpy as np

import pyarrow
from pyarrow.compat import builtin_pickle
from pyarrow.lib import (SerializationContext, _default_serialization_context,
                         py_buffer)

try:
    import cloudpickle
except ImportError:
    cloudpickle = builtin_pickle


# ----------------------------------------------------------------------
# Set up serialization for numpy with dtype object (primitive types are
# handled efficiently with Arrow's Tensor facilities, see
# python_to_arrow.cc)

def _serialize_numpy_array_list(obj):
    if obj.dtype.str != '|O':
        # Make the array c_contiguous if necessary so that we can call change
        # the view.
        if not obj.flags.c_contiguous:
            obj = np.ascontiguousarray(obj)
        return obj.view('uint8'), obj.dtype.str
    else:
        return obj.tolist(), obj.dtype.str


def _deserialize_numpy_array_list(data):
    if data[1] != '|O':
        assert data[0].dtype == np.uint8
        return data[0].view(data[1])
    else:
        return np.array(data[0], dtype=np.dtype(data[1]))


# ----------------------------------------------------------------------
# pyarrow.RecordBatch-specific serialization matters

def _serialize_pyarrow_recordbatch(batch):
    output_stream = pyarrow.BufferOutputStream()
    writer = pyarrow.RecordBatchStreamWriter(output_stream,
                                             schema=batch.schema)
    writer.write_batch(batch)
    writer.close()
    return output_stream.getvalue()  # This will also close the stream.


def _deserialize_pyarrow_recordbatch(buf):
    reader = pyarrow.RecordBatchStreamReader(buf)
    batch = reader.read_next_batch()
    return batch


# ----------------------------------------------------------------------
# pyarrow.Array-specific serialization matters

def _serialize_pyarrow_array(array):
    # TODO(suquark): implement more effcient array serialization.
    batch = pyarrow.RecordBatch.from_arrays([array], [''])
    return _serialize_pyarrow_recordbatch(batch)


def _deserialize_pyarrow_array(buf):
    # TODO(suquark): implement more effcient array deserialization.
    batch = _deserialize_pyarrow_recordbatch(buf)
    return batch.columns[0]


# ----------------------------------------------------------------------
# pyarrow.Table-specific serialization matters

def _serialize_pyarrow_table(table):
    output_stream = pyarrow.BufferOutputStream()
    writer = pyarrow.RecordBatchStreamWriter(output_stream,
                                             schema=table.schema)
    writer.write_table(table)
    writer.close()
    return output_stream.getvalue()  # This will also close the stream.


def _deserialize_pyarrow_table(buf):
    reader = pyarrow.RecordBatchStreamReader(buf)
    table = reader.read_all()
    return table


def _pickle_to_buffer(x):
    pickled = builtin_pickle.dumps(x, protocol=builtin_pickle.HIGHEST_PROTOCOL)
    return py_buffer(pickled)


def _load_pickle_from_buffer(data):
    as_memoryview = memoryview(data)
    if six.PY2:
        return builtin_pickle.loads(as_memoryview.tobytes())
    else:
        return builtin_pickle.loads(as_memoryview)


# ----------------------------------------------------------------------
# pandas-specific serialization matters

def _register_custom_pandas_handlers(context):
    # ARROW-1784, faster path for pandas-only visibility

    try:
        import pandas as pd
    except ImportError:
        return

    import pyarrow.pandas_compat as pdcompat

    sparse_type_error_msg = (
        '{0} serialization is not supported.\n'
        'Note that {0} is planned to be deprecated '
        'in pandas future releases.\n'
        'See https://github.com/pandas-dev/pandas/issues/19239 '
        'for more information.'
    )

    def _serialize_pandas_dataframe(obj):
        if isinstance(obj, pd.SparseDataFrame):
            raise NotImplementedError(
                sparse_type_error_msg.format('SparseDataFrame')
            )

        return pdcompat.dataframe_to_serialized_dict(obj)

    def _deserialize_pandas_dataframe(data):
        return pdcompat.serialized_dict_to_dataframe(data)

    def _serialize_pandas_series(obj):
        if isinstance(obj, pd.SparseSeries):
            raise NotImplementedError(
                sparse_type_error_msg.format('SparseSeries')
            )

        return _serialize_pandas_dataframe(pd.DataFrame({obj.name: obj}))

    def _deserialize_pandas_series(data):
        deserialized = _deserialize_pandas_dataframe(data)
        return deserialized[deserialized.columns[0]]

    context.register_type(
        pd.Series, 'pd.Series',
        custom_serializer=_serialize_pandas_series,
        custom_deserializer=_deserialize_pandas_series)

    context.register_type(
        pd.Index, 'pd.Index',
        custom_serializer=_pickle_to_buffer,
        custom_deserializer=_load_pickle_from_buffer)

    if hasattr(pd.core.arrays, 'interval'):
        context.register_type(
            pd.core.arrays.interval.IntervalArray,
            'pd.core.arrays.interval.IntervalArray',
            custom_serializer=_pickle_to_buffer,
            custom_deserializer=_load_pickle_from_buffer)

    if hasattr(pd.core.arrays, 'period'):
        context.register_type(
            pd.core.arrays.period.PeriodArray,
            'pd.core.arrays.period.PeriodArray',
            custom_serializer=_pickle_to_buffer,
            custom_deserializer=_load_pickle_from_buffer)

    if hasattr(pd.core.arrays, 'datetimes'):
        context.register_type(
            pd.core.arrays.datetimes.DatetimeArray,
            'pd.core.arrays.datetimes.DatetimeArray',
            custom_serializer=_pickle_to_buffer,
            custom_deserializer=_load_pickle_from_buffer)

    context.register_type(
        pd.DataFrame, 'pd.DataFrame',
        custom_serializer=_serialize_pandas_dataframe,
        custom_deserializer=_deserialize_pandas_dataframe)


def register_torch_serialization_handlers(serialization_context):
    # ----------------------------------------------------------------------
    # Set up serialization for pytorch tensors

    try:
        import torch

        def _serialize_torch_tensor(obj):
            return obj.detach().numpy()

        def _deserialize_torch_tensor(data):
            return torch.from_numpy(data)

        for t in [torch.FloatTensor, torch.DoubleTensor, torch.HalfTensor,
                  torch.ByteTensor, torch.CharTensor, torch.ShortTensor,
                  torch.IntTensor, torch.LongTensor, torch.Tensor]:
            serialization_context.register_type(
                t, "torch." + t.__name__,
                custom_serializer=_serialize_torch_tensor,
                custom_deserializer=_deserialize_torch_tensor)
    except ImportError:
        # no torch
        pass


def register_default_serialization_handlers(serialization_context):

    # ----------------------------------------------------------------------
    # Set up serialization for primitive datatypes

    # TODO(pcm): This is currently a workaround until arrow supports
    # arbitrary precision integers. This is only called on long integers,
    # see the associated case in the append method in python_to_arrow.cc
    serialization_context.register_type(
        int, "int",
        custom_serializer=lambda obj: str(obj),
        custom_deserializer=lambda data: int(data))

    if (sys.version_info < (3, 0)):
        serialization_context.register_type(
            long, "long",  # noqa: F821
            custom_serializer=lambda obj: str(obj),
            custom_deserializer=lambda data: long(data))  # noqa: F821

    def _serialize_ordered_dict(obj):
        return list(obj.keys()), list(obj.values())

    def _deserialize_ordered_dict(data):
        return OrderedDict(zip(data[0], data[1]))

    serialization_context.register_type(
        OrderedDict, "OrderedDict",
        custom_serializer=_serialize_ordered_dict,
        custom_deserializer=_deserialize_ordered_dict)

    def _serialize_default_dict(obj):
        return list(obj.keys()), list(obj.values()), obj.default_factory

    def _deserialize_default_dict(data):
        return defaultdict(data[2], zip(data[0], data[1]))

    serialization_context.register_type(
        defaultdict, "defaultdict",
        custom_serializer=_serialize_default_dict,
        custom_deserializer=_deserialize_default_dict)

    serialization_context.register_type(
        type(lambda: 0), "function",
        pickle=True)

    serialization_context.register_type(type, "type", pickle=True)

    serialization_context.register_type(
        np.ndarray, 'np.array',
        custom_serializer=_serialize_numpy_array_list,
        custom_deserializer=_deserialize_numpy_array_list)

    serialization_context.register_type(
        pyarrow.Array, 'pyarrow.Array',
        custom_serializer=_serialize_pyarrow_array,
        custom_deserializer=_deserialize_pyarrow_array)

    serialization_context.register_type(
        pyarrow.RecordBatch, 'pyarrow.RecordBatch',
        custom_serializer=_serialize_pyarrow_recordbatch,
        custom_deserializer=_deserialize_pyarrow_recordbatch)

    serialization_context.register_type(
        pyarrow.Table, 'pyarrow.Table',
        custom_serializer=_serialize_pyarrow_table,
        custom_deserializer=_deserialize_pyarrow_table)

    _register_custom_pandas_handlers(serialization_context)


def default_serialization_context():
    context = SerializationContext()
    register_default_serialization_handlers(context)
    return context


register_default_serialization_handlers(_default_serialization_context)
