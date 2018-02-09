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

from pyarrow.compat import builtin_pickle
from pyarrow.lib import (SerializationContext, _default_serialization_context,
                         frombuffer)

try:
    import cloudpickle
except ImportError:
    cloudpickle = builtin_pickle


# ----------------------------------------------------------------------
# Set up serialization for numpy with dtype object (primitive types are
# handled efficiently with Arrow's Tensor facilities, see
# python_to_arrow.cc)

def _serialize_numpy_array_list(obj):
    return obj.tolist(), obj.dtype.str


def _deserialize_numpy_array_list(data):
    return np.array(data[0], dtype=np.dtype(data[1]))


def _pickle_to_buffer(x):
    pickled = builtin_pickle.dumps(x, protocol=builtin_pickle.HIGHEST_PROTOCOL)
    return frombuffer(pickled)


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

    def _serialize_pandas_dataframe(obj):
        return pdcompat.dataframe_to_serialized_dict(obj)

    def _deserialize_pandas_dataframe(data):
        return pdcompat.serialized_dict_to_dataframe(data)

    def _serialize_pandas_series(obj):
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
            return obj.numpy()

        def _deserialize_torch_tensor(data):
            return torch.from_numpy(data)

        for t in [torch.FloatTensor, torch.DoubleTensor, torch.HalfTensor,
                  torch.ByteTensor, torch.CharTensor, torch.ShortTensor,
                  torch.IntTensor, torch.LongTensor]:
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

    _register_custom_pandas_handlers(serialization_context)


def default_serialization_context():
    context = SerializationContext()
    register_default_serialization_handlers(context)
    return context


register_default_serialization_handlers(_default_serialization_context)
