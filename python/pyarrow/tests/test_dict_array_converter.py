import pytest

import pyarrow as pa


def test_arrow_missing_function():

    pa.array([], pa.dictionary(pa.int32(), pa.string()))
    pa.array([], pa.dictionary(pa.int32(), pa.binary()))

    pa.array([], pa.dictionary(pa.int32(), pa.large_string()))
    pa.array([], pa.dictionary(pa.int32(), pa.large_binary()))
