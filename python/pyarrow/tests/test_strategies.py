import hypothesis as h
import hypothesis.strategies as st

import pyarrow as pa
import pyarrow.tests.strategies as past


@h.given(past.all_types)
def test_types(ty):
    assert isinstance(ty, pa.lib.DataType)


@h.given(past.all_fields)
def test_fields(field):
    assert isinstance(field, pa.lib.Field)


@h.given(past.all_schemas)
def test_schemas(schema):
    assert isinstance(schema, pa.lib.Schema)


@h.given(past.all_arrays)
def test_arrays(array):
    assert isinstance(array, pa.lib.Array)


@h.given(past.all_chunked_arrays)
def test_chunked_arrays(chunked_array):
    assert isinstance(chunked_array, pa.lib.ChunkedArray)


@h.given(past.all_columns)
def test_columns(column):
    assert isinstance(column, pa.lib.Column)


@h.given(past.all_record_batches)
def test_record_batches(record_bath):
    assert isinstance(record_bath, pa.lib.RecordBatch)


############################################################


@h.given(st.text(), past.all_arrays | past.all_chunked_arrays)
def test_column_factory(name, arr):
    column = pa.column(name, arr)
    assert isinstance(column, pa.Column)
