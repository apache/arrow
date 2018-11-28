import hypothesis.strategies as st

import pyarrow as pa


# TODO alphanum_text, surrogate_text
custom_text = st.text(
    alphabet=st.characters(
        min_codepoint=int('41', 16),
        max_codepoint=int('7E', 16)
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
timestamp_types = st.sampled_from([
    pa.timestamp('s'),
    pa.timestamp('ms'),
    pa.timestamp('us'),
    pa.timestamp('ns')
])
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


@st.defines_strategy
def fields(type_strategy=primitive_types):
    return st.builds(pa.field, name=custom_text, type=type_strategy,
                     nullable=st.booleans(), metadata=metadata)


@st.defines_strategy
def list_types(item_strategy=primitive_types):
    return st.builds(pa.list_, item_strategy)


@st.defines_strategy
def struct_types(item_strategy=primitive_types):
    return st.builds(pa.struct, st.lists(fields(item_strategy)))


@st.defines_strategy
def complex_types(inner_strategy=primitive_types):
    return list_types(inner_strategy) | struct_types(inner_strategy)


@st.defines_strategy
def nested_list_types(item_strategy=primitive_types):
    return st.recursive(item_strategy, list_types)


@st.defines_strategy
def nested_struct_types(item_strategy=primitive_types):
    return st.recursive(item_strategy, struct_types)


@st.defines_strategy
def nested_complex_types(inner_strategy=primitive_types):
    return st.recursive(inner_strategy, complex_types)


all_types = st.one_of(
    primitive_types,
    list_types(),
    struct_types(),
    complex_types(),
    nested_list_types(),
    nested_struct_types(),
    nested_complex_types()
)


@st.defines_strategy
def schemas(type_strategy=primitive_types):
    return st.builds(pa.schema, st.lists(fields(type_strategy)))


complex_schemas = schemas(complex_types())
