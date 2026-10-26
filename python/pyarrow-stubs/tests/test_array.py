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
# mypy: warn-unused-ignores

from typing import Any, cast

import pyarrow.lib as lib

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check


def test_array() -> None:
    # pa.array() isn't annotated with a specific return type yet, so tell
    # the type checker which class to expect.
    arr = check(cast(lib.Int64Array, lib.array([1, 2, None])), lib.Int64Array)

    check(assert_type(arr.type, lib.DataType), lib.DataType)
    check(assert_type(arr.null_count, int), int)
    check(assert_type(len(arr), int), int)
    check(assert_type(arr.to_pylist(), list[Any]), list)

    # Methods returning the same kind of array keep the subclass.
    check(assert_type(arr.slice(1), lib.Int64Array), lib.Int64Array)
    check(assert_type(arr.fill_null(0), lib.Int64Array), lib.Int64Array)
    check(assert_type(arr.drop_null(), lib.Int64Array), lib.Int64Array)

    check(assert_type(arr.is_null(), lib.BooleanArray), lib.BooleanArray)
    check(assert_type(arr.cast(lib.float64()), lib.Array), lib.DoubleArray)
    check(assert_type(arr.cast("float64"), lib.Array), lib.DoubleArray)
    check(assert_type(lib.concat_arrays([arr, arr]), lib.Array),
          lib.Int64Array)


def test_dictionary_array() -> None:
    arr = check(cast(lib.StringArray, lib.array(["a", "b", "a"])),
                lib.StringArray)
    dict_arr = arr.dictionary_encode()
    check(assert_type(dict_arr, lib.DictionaryArray), lib.DictionaryArray)
    check(assert_type(dict_arr.indices, lib.IntegerArray), lib.Int32Array)
    check(assert_type(dict_arr.dictionary, lib.Array), lib.StringArray)
    check(assert_type(dict_arr.dictionary_decode(), lib.Array),
          lib.StringArray)


def test_run_end_encoded_array() -> None:
    ree = lib.RunEndEncodedArray.from_arrays([2, 3], ["a", "b"])
    check(assert_type(ree, lib.RunEndEncodedArray), lib.RunEndEncodedArray)
    check(
        assert_type(ree.run_ends,
                    lib.Int16Array | lib.Int32Array | lib.Int64Array),
        lib.Int64Array,
    )
    check(assert_type(ree.values, lib.Array), lib.StringArray)


def test_chunked_array() -> None:
    chunked = lib.chunked_array([[1, 2], [3]])
    check(assert_type(chunked, lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked.type, lib.DataType), lib.DataType)
    check(assert_type(chunked.num_chunks, int), int)
    check(assert_type(chunked.chunk(0), lib.Array), lib.Int64Array)
    check(assert_type(chunked.chunks, list[lib.Array]), list)
    check(assert_type(chunked.combine_chunks(), lib.Array), lib.Int64Array)
    check(assert_type(chunked.slice(1), lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked.cast(lib.float64()), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(chunked.is_null(), lib.ChunkedArray), lib.ChunkedArray)


def test_invalid_usage() -> None:
    arr = cast(lib.Int64Array, lib.array([1, 2]))
    if TYPE_CHECKING_INVALID_USAGE:
        arr.cast(1)  # type: ignore[arg-type]
        arr.slice("1")  # type: ignore[arg-type]
