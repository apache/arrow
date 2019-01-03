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
    precision=st.integers(min_value=0, max_value=38),
    scale=st.integers(min_value=0, max_value=38)
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
temporal_types = st.one_of(date_types, time_types, timestamp_types)

primitive_types = st.one_of(
    null_type,
    bool_type,
    binary_type,
    string_type,
    numeric_types,
    temporal_types
)

metadata = st.dictionaries(st.text(), st.text())


def fields(type_strategy=primitive_types):
    return st.builds(pa.field, name=custom_text, type=type_strategy,
                     nullable=st.booleans(), metadata=metadata)


def list_types(item_strategy=primitive_types):
    return st.builds(pa.list_, item_strategy)


def struct_types(item_strategy=primitive_types):
    return st.builds(pa.struct, st.lists(fields(item_strategy)))


def complex_types(inner_strategy=primitive_types):
    return list_types(inner_strategy) | struct_types(inner_strategy)


def nested_list_types(item_strategy=primitive_types):
    return st.recursive(item_strategy, list_types)


def nested_struct_types(item_strategy=primitive_types):
    return st.recursive(item_strategy, struct_types)


def nested_complex_types(inner_strategy=primitive_types):
    return st.recursive(inner_strategy, complex_types)


def schemas(type_strategy=primitive_types):
    return st.builds(pa.schema, st.lists(fields(type_strategy)))


complex_schemas = schemas(complex_types())


all_types = st.one_of(primitive_types, complex_types(), nested_complex_types())
all_fields = fields(all_types)
all_schemas = schemas(all_types)


@st.composite
def arrays(draw, type, size):
    if isinstance(type, st.SearchStrategy):
        type = draw(type)
    if isinstance(size, st.SearchStrategy):
        size = draw(size)

    if not isinstance(type, pa.DataType):
        raise TypeError('Type must be a pyarrow DataType')
    if not isinstance(size, int):
        raise TypeError('Size must be an integer')

    shape = (size,)

    if pa.types.is_list(type):
        offsets = draw(npst.arrays(np.uint8(), shape=shape)).cumsum() // 20
        offsets = np.insert(offsets, 0, 0, axis=0)  # prepend with zero
        values = draw(arrays(type.value_type, size=int(offsets.sum())))
        return pa.ListArray.from_arrays(offsets, values)

    if pa.types.is_struct(type):
        h.assume(len(type) > 0)  # TODO issue pa.struct([])
        names, child_arrays = [], []
        for field in type:
            names.append(field.name)
            child_arrays.append(draw(arrays(field.type, size=size)))
        return pa.StructArray.from_arrays(child_arrays, names=names)

    if (pa.types.is_boolean(type) or pa.types.is_integer(type) or
            pa.types.is_floating(type)):
        values = npst.arrays(type.to_pandas_dtype(), shape=(size,))
        return pa.array(draw(values), type=type)

    if pa.types.is_null(type):
        value = st.none()
    elif pa.types.is_time(type):
        value = st.times()
    elif pa.types.is_date(type):
        value = st.dates()
    elif pa.types.is_timestamp(type):
        tz = pytz.timezone(type.tz) if type.tz is not None else None
        value = st.datetimes(timezones=st.just(tz))
    elif pa.types.is_binary(type):
        value = st.binary()
    elif pa.types.is_string(type):
        value = st.text()
    elif pa.types.is_decimal(type):
        # FIXME(kszucs): properly limit the precision
        value = st.decimals(places=type.scale, allow_infinity=False)
        type = None  # We let arrow infer it from the values
    else:
        raise NotImplementedError(type)

    values = st.lists(value, min_size=size, max_size=size)
    return pa.array(draw(values), type=type)
