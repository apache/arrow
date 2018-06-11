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
import pytest

import pyarrow as pa


def test_get_include():
    include_dir = pa.get_include()
    assert os.path.exists(os.path.join(include_dir, 'arrow', 'api.h'))


def test_cpu_count():
    n = pa.cpu_count()
    assert n > 0
    try:
        pa.set_cpu_count(n + 5)
        assert pa.cpu_count() == n + 5
    finally:
        pa.set_cpu_count(n)


@pytest.mark.parametrize('klass', [
    pa.Field,
    pa.Schema,
    pa.Column,
    pa.ChunkedArray,
    pa.RecordBatch,
    pa.Table,
    pa.Buffer,
    pa.Array,
    pa.Tensor,
    pa.lib.DataType,
    pa.lib.ListType,
    pa.lib.UnionType,
    pa.lib.StructType,
    pa.lib.Time32Type,
    pa.lib.Time64Type,
    pa.lib.TimestampType,
    pa.lib.Decimal128Type,
    pa.lib.DictionaryType,
    pa.lib.FixedSizeBinaryType,
    pa.NullArray,
    pa.NumericArray,
    pa.IntegerArray,
    pa.FloatingPointArray,
    pa.BooleanArray,
    pa.Int8Array,
    pa.Int16Array,
    pa.Int32Array,
    pa.Int64Array,
    pa.UInt8Array,
    pa.UInt16Array,
    pa.UInt32Array,
    pa.UInt64Array,
    pa.ListArray,
    pa.UnionArray,
    pa.BinaryArray,
    pa.StringArray,
    pa.FixedSizeBinaryArray,
    pa.DictionaryArray,
    pa.Date32Array,
    pa.Date64Array,
    pa.TimestampArray,
    pa.Time32Array,
    pa.Time64Array,
    pa.Decimal128Array,
    pa.StructArray,
    pa.ArrayValue,
    pa.BooleanValue,
    pa.Int8Value,
    pa.Int16Value,
    pa.Int32Value,
    pa.Int64Value,
    pa.UInt8Value,
    pa.UInt16Value,
    pa.UInt32Value,
    pa.UInt64Value,
    pa.HalfFloatValue,
    pa.FloatValue,
    pa.DoubleValue,
    pa.DecimalValue,
    pa.Date32Value,
    pa.Date64Value,
    pa.Time32Value,
    pa.Time64Value,
    pa.TimestampValue,
    pa.StringValue,
    pa.BinaryValue,
    pa.FixedSizeBinaryValue,
    pa.ListValue,
    pa.UnionValue,
    pa.StructValue,
    pa.DictionaryValue
])
def test_extension_type_constructor_errors(klass):
    # ARROW-2638: prevent calling extension class constructors directly
    msg = "Do not call {cls}'s constructor directly, use .* instead."
    with pytest.raises(TypeError, match=msg.format(cls=klass.__name__)):
        klass()
