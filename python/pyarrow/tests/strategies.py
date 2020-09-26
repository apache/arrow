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

import datetime

import pytz
import hypothesis as h
import hypothesis.strategies as st
import hypothesis.extra.numpy as npst
import hypothesis.extra.pytz as tzst
import numpy as np

import pyarrow as pa


# TODO(kszucs): alphanum_text, surrogate_text
custom_text = st.text(
    alphabet=st.characters(
        min_codepoint=0x41,
        max_codepoint=0x7E
    )
)

null_type = st.just(pa.null())
bool_type = st.just(pa.bool_())

binary_type = st.just(pa.binary())
string_type = st.just(pa.string())
large_binary_type = st.just(pa.large_binary())
large_string_type = st.just(pa.large_string())

signed_integer_types = st.sampled_from([
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64()
])
unsigned_integer_types = st.sampled_from([
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64()
])
integer_types = st.one_of(signed_integer_types, unsigned_integer_types)

floating_types = st.sampled_from([
    pa.float16(),
    pa.float32(),
    pa.float64()
])
decimal_type = st.builds(
    pa.decimal128,
    precision=st.integers(min_value=1, max_value=38),
    scale=st.integers(min_value=1, max_value=38)
)
numeric_types = st.one_of(integer_types, floating_types, decimal_type)

date_types = st.sampled_from([
    pa.date32(),
    pa.date64()
])
time_types = st.sampled_from([
    pa.time32('s'),
    pa.time32('ms'),
    pa.time64('us'),
    pa.time64('ns')
])
timestamp_types = st.builds(
    pa.timestamp,
    unit=st.sampled_from(['s', 'ms', 'us', 'ns']),
    tz=tzst.timezones()
)
duration_types = st.builds(
    pa.duration,
    st.sampled_from(['s', 'ms', 'us', 'ns'])
)
temporal_types = st.one_of(
    date_types,
    time_types,
    timestamp_types,
    duration_types
)

primitive_types = st.one_of(
    null_type,
    bool_type,
    binary_type,
    string_type,
    large_binary_type,
    large_string_type,
    numeric_types,
    temporal_types
)

metadata = st.dictionaries(st.text(), st.text())


@st.composite
def fields(draw, type_strategy=primitive_types):
    name = draw(custom_text)
    typ = draw(type_strategy)
    if pa.types.is_null(typ):
        nullable = True
    else:
        nullable = draw(st.booleans())
    meta = draw(metadata)
    return pa.field(name, type=typ, nullable=nullable, metadata=meta)


def list_types(item_strategy=primitive_types):
    return (
        st.builds(pa.list_, item_strategy) |
        st.builds(pa.large_list, item_strategy)
    )


def struct_types(item_strategy=primitive_types):
    return st.builds(pa.struct, st.lists(fields(item_strategy)))


def complex_types(inner_strategy=primitive_types):
    return list_types(inner_strategy) | struct_types(inner_strategy)


def nested_list_types(item_strategy=primitive_types, max_leaves=3):
    return st.recursive(item_strategy, list_types, max_leaves=max_leaves)


def nested_struct_types(item_strategy=primitive_types, max_leaves=3):
    return st.recursive(item_strategy, struct_types, max_leaves=max_leaves)


def nested_complex_types(inner_strategy=primitive_types, max_leaves=3):
    return st.recursive(inner_strategy, complex_types, max_leaves=max_leaves)


def schemas(type_strategy=primitive_types, max_fields=None):
    children = st.lists(fields(type_strategy), max_size=max_fields)
    return st.builds(pa.schema, children)


complex_schemas = schemas(complex_types())


all_types = st.one_of(primitive_types, complex_types(), nested_complex_types())
all_fields = fields(all_types)
all_schemas = schemas(all_types)


_default_array_sizes = st.integers(min_value=0, max_value=20)


@st.composite
def arrays(draw, type, size=None):
    if isinstance(type, st.SearchStrategy):
        ty = draw(type)
    elif isinstance(type, pa.DataType):
        ty = type
    else:
        raise TypeError('Type must be a pyarrow DataType')

    if isinstance(size, st.SearchStrategy):
        size = draw(size)
    elif size is None:
        size = draw(_default_array_sizes)
    elif not isinstance(size, int):
        raise TypeError('Size must be an integer')

    shape = (size,)

    if pa.types.is_list(ty) or pa.types.is_large_list(ty):
        offsets = draw(npst.arrays(np.uint8(), shape=shape)).cumsum() // 20
        offsets = np.insert(offsets, 0, 0, axis=0)  # prepend with zero
        values = draw(arrays(ty.value_type, size=int(offsets.sum())))
        if pa.types.is_large_list(ty):
            array_type = pa.LargeListArray
        else:
            array_type = pa.ListArray
        return array_type.from_arrays(offsets, values)

    if pa.types.is_struct(ty):
        h.assume(len(ty) > 0)
        fields, child_arrays = [], []
        for field in ty:
            fields.append(field)
            child_arrays.append(draw(arrays(field.type, size=size)))
        return pa.StructArray.from_arrays(child_arrays, fields=fields)

    if (pa.types.is_boolean(ty) or pa.types.is_integer(ty) or
            pa.types.is_floating(ty)):
        values = npst.arrays(ty.to_pandas_dtype(), shape=(size,))
        np_arr = draw(values)
        if pa.types.is_floating(ty):
            # Workaround ARROW-4952: no easy way to assert array equality
            # in a NaN-tolerant way.
            np_arr[np.isnan(np_arr)] = -42.0
        return pa.array(np_arr, type=ty)

    if pa.types.is_null(ty):
        value = st.none()
    elif pa.types.is_time(ty):
        value = st.times()
    elif pa.types.is_date(ty):
        value = st.dates()
    elif pa.types.is_timestamp(ty):
        min_int64 = -(2**63)
        max_int64 = 2**63 - 1
        min_datetime = datetime.datetime.fromtimestamp(min_int64 / 10**9)
        max_datetime = datetime.datetime.fromtimestamp(max_int64 / 10**9)
        try:
            offset_hours = int(ty.tz)
            tz = pytz.FixedOffset(offset_hours * 60)
        except ValueError:
            tz = pytz.timezone(ty.tz)
        value = st.datetimes(timezones=st.just(tz), min_value=min_datetime,
                             max_value=max_datetime)
    elif pa.types.is_duration(ty):
        value = st.timedeltas()
    elif pa.types.is_binary(ty) or pa.types.is_large_binary(ty):
        value = st.binary()
    elif pa.types.is_string(ty) or pa.types.is_large_string(ty):
        value = st.text()
    elif pa.types.is_decimal(ty):
        # TODO(kszucs): properly limit the precision
        # value = st.decimals(places=type.scale, allow_infinity=False)
        h.reject()
    else:
        raise NotImplementedError(ty)

    values = st.lists(value, min_size=size, max_size=size)
    return pa.array(draw(values), type=ty)


@st.composite
def chunked_arrays(draw, type, min_chunks=0, max_chunks=None, chunk_size=None):
    if isinstance(type, st.SearchStrategy):
        type = draw(type)

    # TODO(kszucs): remove it, field metadata is not kept
    h.assume(not pa.types.is_struct(type))

    chunk = arrays(type, size=chunk_size)
    chunks = st.lists(chunk, min_size=min_chunks, max_size=max_chunks)

    return pa.chunked_array(draw(chunks), type=type)


@st.composite
def record_batches(draw, type, rows=None, max_fields=None):
    if isinstance(rows, st.SearchStrategy):
        rows = draw(rows)
    elif rows is None:
        rows = draw(_default_array_sizes)
    elif not isinstance(rows, int):
        raise TypeError('Rows must be an integer')

    schema = draw(schemas(type, max_fields=max_fields))
    children = [draw(arrays(field.type, size=rows)) for field in schema]
    # TODO(kszucs): the names and schema arguments are not consistent with
    #               Table.from_array's arguments
    return pa.RecordBatch.from_arrays(children, names=schema)


@st.composite
def tables(draw, type, rows=None, max_fields=None):
    if isinstance(rows, st.SearchStrategy):
        rows = draw(rows)
    elif rows is None:
        rows = draw(_default_array_sizes)
    elif not isinstance(rows, int):
        raise TypeError('Rows must be an integer')

    schema = draw(schemas(type, max_fields=max_fields))
    children = [draw(arrays(field.type, size=rows)) for field in schema]
    return pa.Table.from_arrays(children, schema=schema)


all_arrays = arrays(all_types)
all_chunked_arrays = chunked_arrays(all_types)
all_record_batches = record_batches(all_types)
all_tables = tables(all_types)
