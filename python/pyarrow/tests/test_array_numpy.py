import datetime
import decimal
import struct
import sys

import pytest
import hypothesis as h
import hypothesis.strategies as st

import pyarrow as pa
import pyarrow.tests.strategies as past
from pyarrow.vendored.version import Version

try:
    import numpy as np
except ImportError:
    np = None


def _check_cast_case(case, *, safe=True, check_array_construction=True):
    in_data, in_type, out_data, out_type = case
    if isinstance(out_data, pa.Array):
        assert out_data.type == out_type
        expected = out_data
    else:
        expected = pa.array(out_data, type=out_type)

    # check casting an already created array
    if isinstance(in_data, pa.Array):
        assert in_data.type == in_type
        in_arr = in_data
    else:
        in_arr = pa.array(in_data, type=in_type)
    casted = in_arr.cast(out_type, safe=safe)
    casted.validate(full=True)
    assert casted.equals(expected)

    # constructing an array with out type which optionally involves casting
    # for more see ARROW-1949
    if check_array_construction:
        in_arr = pa.array(in_data, type=out_type, safe=safe)
        assert in_arr.equals(expected)


@pytest.mark.numpy
def test_to_numpy_zero_copy():
    arr = pa.array(range(10))

    np_arr = arr.to_numpy()

    # check for zero copy (both arrays using same memory)
    arrow_buf = arr.buffers()[1]
    assert arrow_buf.address == np_arr.ctypes.data

    arr = None
    import gc
    gc.collect()

    # Ensure base is still valid
    assert np_arr.base is not None
    expected = np.arange(10)
    np.testing.assert_array_equal(np_arr, expected)


@pytest.mark.numpy
def test_chunked_array_to_numpy_zero_copy():
    elements = [[2, 2, 4], [4, 5, 100]]

    chunked_arr = pa.chunked_array(elements)

    msg = "zero_copy_only must be False for pyarrow.ChunkedArray.to_numpy"

    with pytest.raises(ValueError, match=msg):
        chunked_arr.to_numpy(zero_copy_only=True)

    np_arr = chunked_arr.to_numpy()
    expected = [2, 2, 4, 4, 5, 100]
    np.testing.assert_array_equal(np_arr, expected)


@pytest.mark.numpy
def test_to_numpy_unsupported_types():
    # ARROW-2871: Some primitive types are not yet supported in to_numpy
    bool_arr = pa.array([True, False, True])

    with pytest.raises(ValueError):
        bool_arr.to_numpy()

    result = bool_arr.to_numpy(zero_copy_only=False)
    expected = np.array([True, False, True])
    np.testing.assert_array_equal(result, expected)

    null_arr = pa.array([None, None, None])

    with pytest.raises(ValueError):
        null_arr.to_numpy()

    result = null_arr.to_numpy(zero_copy_only=False)
    expected = np.array([None, None, None], dtype=object)
    np.testing.assert_array_equal(result, expected)

    arr = pa.array([1, 2, None])

    with pytest.raises(ValueError, match="with 1 nulls"):
        arr.to_numpy()


@pytest.mark.numpy
def test_to_numpy_writable():
    arr = pa.array(range(10))
    np_arr = arr.to_numpy()

    # by default not writable for zero-copy conversion
    with pytest.raises(ValueError):
        np_arr[0] = 10

    np_arr2 = arr.to_numpy(zero_copy_only=False, writable=True)
    np_arr2[0] = 10
    assert arr[0].as_py() == 0

    # when asking for writable, cannot do zero-copy
    with pytest.raises(ValueError):
        arr.to_numpy(zero_copy_only=True, writable=True)


@pytest.mark.numpy
@pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
@pytest.mark.parametrize('tz', [None, "UTC"])
def test_to_numpy_datetime64(unit, tz):
    arr = pa.array([1, 2, 3], pa.timestamp(unit, tz=tz))
    expected = np.array([1, 2, 3], dtype=f"datetime64[{unit}]")
    np_arr = arr.to_numpy()
    np.testing.assert_array_equal(np_arr, expected)


@pytest.mark.numpy
@pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
def test_to_numpy_timedelta64(unit):
    arr = pa.array([1, 2, 3], pa.duration(unit))
    expected = np.array([1, 2, 3], dtype=f"timedelta64[{unit}]")
    np_arr = arr.to_numpy()
    np.testing.assert_array_equal(np_arr, expected)


@pytest.mark.numpy
def test_to_numpy_dictionary():
    # ARROW-7591
    arr = pa.array(["a", "b", "a"]).dictionary_encode()
    expected = np.array(["a", "b", "a"], dtype=object)
    np_arr = arr.to_numpy(zero_copy_only=False)
    np.testing.assert_array_equal(np_arr, expected)


@pytest.mark.numpy
def test_array_getitem_numpy_scalars():
    arr = pa.array(range(10, 15))
    lst = arr.to_pylist()
    # check that numpy scalars are supported
    for idx in range(-len(arr), len(arr)):
        assert arr[np.int32(idx)].as_py() == lst[idx]


@pytest.mark.numpy
def test_array_factory_invalid_type():

    class MyObject:
        pass

    arr = np.array([MyObject()])
    with pytest.raises(ValueError):
        pa.array(arr)


@pytest.mark.numpy
def test_array_ref_to_ndarray_base():
    arr = np.array([1, 2, 3])

    refcount = sys.getrefcount(arr)
    arr2 = pa.array(arr)  # noqa
    assert sys.getrefcount(arr) == (refcount + 1)


@pytest.mark.numpy
def test_array_from_buffers():
    values_buf = pa.py_buffer(np.int16([4, 5, 6, 7]))
    nulls_buf = pa.py_buffer(np.uint8([0b00001101]))
    arr = pa.Array.from_buffers(pa.int16(), 4, [nulls_buf, values_buf])
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [4, None, 6, 7]

    arr = pa.Array.from_buffers(pa.int16(), 4, [None, values_buf])
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [4, 5, 6, 7]

    arr = pa.Array.from_buffers(pa.int16(), 3, [nulls_buf, values_buf],
                                offset=1)
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [None, 6, 7]

    with pytest.raises(TypeError):
        pa.Array.from_buffers(pa.int16(), 3, ['', ''], offset=1)


@pytest.mark.numpy
def test_dictionary_from_numpy():
    indices = np.repeat([0, 1, 2], 2)
    dictionary = np.array(['foo', 'bar', 'baz'], dtype=object)
    mask = np.array([False, False, True, False, False, False])

    d1 = pa.DictionaryArray.from_arrays(indices, dictionary)
    d2 = pa.DictionaryArray.from_arrays(indices, dictionary, mask=mask)

    assert d1.indices.to_pylist() == indices.tolist()
    assert d1.indices.to_pylist() == indices.tolist()
    assert d1.dictionary.to_pylist() == dictionary.tolist()
    assert d2.dictionary.to_pylist() == dictionary.tolist()

    for i in range(len(indices)):
        assert d1[i].as_py() == dictionary[indices[i]]

        if mask[i]:
            assert d2[i].as_py() is None
        else:
            assert d2[i].as_py() == dictionary[indices[i]]


@pytest.mark.numpy
def test_dictionary_to_numpy():
    expected = pa.array(
        ["foo", "bar", None, "foo"]
    ).to_numpy(zero_copy_only=False)
    a = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0]),
        pa.array(['foo', 'bar'])
    )
    np.testing.assert_array_equal(a.to_numpy(zero_copy_only=False),
                                  expected)

    with pytest.raises(pa.ArrowInvalid):
        # If this would be changed to no longer raise in the future,
        # ensure to test the actual result because, currently, to_numpy takes
        # for granted that when zero_copy_only=True there will be no nulls
        # (it's the decoding of the DictionaryArray that handles the nulls and
        # this is only activated with zero_copy_only=False)
        a.to_numpy(zero_copy_only=True)

    anonulls = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 0]),
        pa.array(['foo', 'bar'])
    )
    expected = pa.array(
        ["foo", "bar", "bar", "foo"]
    ).to_numpy(zero_copy_only=False)
    np.testing.assert_array_equal(anonulls.to_numpy(zero_copy_only=False),
                                  expected)

    with pytest.raises(pa.ArrowInvalid):
        anonulls.to_numpy(zero_copy_only=True)

    afloat = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 0]),
        pa.array([13.7, 11.0])
    )
    expected = pa.array([13.7, 11.0, 11.0, 13.7]).to_numpy()
    np.testing.assert_array_equal(afloat.to_numpy(zero_copy_only=True),
                                  expected)
    np.testing.assert_array_equal(afloat.to_numpy(zero_copy_only=False),
                                  expected)

    afloat2 = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0]),
        pa.array([13.7, 11.0])
    )
    expected = pa.array(
        [13.7, 11.0, None, 13.7]
    ).to_numpy(zero_copy_only=False)
    np.testing.assert_allclose(
        afloat2.to_numpy(zero_copy_only=False),
        expected,
        equal_nan=True
    )

    # Testing for integers can reveal problems related to dealing
    # with None values, as a numpy array of int dtype
    # can't contain NaN nor None.
    aints = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0]),
        pa.array([7, 11])
    )
    expected = pa.array([7, 11, None, 7]).to_numpy(zero_copy_only=False)
    np.testing.assert_allclose(
        aints.to_numpy(zero_copy_only=False),
        expected,
        equal_nan=True
    )


@pytest.mark.numpy
def test_dictionary_from_boxed_arrays():
    indices = np.repeat([0, 1, 2], 2)
    dictionary = np.array(['foo', 'bar', 'baz'], dtype=object)

    iarr = pa.array(indices)
    darr = pa.array(dictionary)

    d1 = pa.DictionaryArray.from_arrays(iarr, darr)

    assert d1.indices.to_pylist() == indices.tolist()
    assert d1.dictionary.to_pylist() == dictionary.tolist()

    for i in range(len(indices)):
        assert d1[i].as_py() == dictionary[indices[i]]


@pytest.mark.numpy
@pytest.mark.parametrize(('list_array_type', 'list_type_factory'),
                         [(pa.ListArray, pa.list_),
                          (pa.LargeListArray, pa.large_list)])
def test_list_from_arrays(list_array_type, list_type_factory):
    offsets_arr = np.array([0, 2, 5, 8], dtype='i4')
    offsets = pa.array(offsets_arr, type='int32')
    pyvalues = [b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h']
    values = pa.array(pyvalues, type='binary')

    result = list_array_type.from_arrays(offsets, values)
    expected = pa.array([pyvalues[:2], pyvalues[2:5], pyvalues[5:8]],
                        type=list_type_factory(pa.binary()))

    assert result.equals(expected)

    # With specified type
    typ = list_type_factory(pa.field("name", pa.binary()))
    result = list_array_type.from_arrays(offsets, values, typ)
    assert result.type == typ
    assert result.type.value_field.name == "name"

    # With nulls
    offsets = [0, None, 2, 6]
    values = [b'a', b'b', b'c', b'd', b'e', b'f']

    result = list_array_type.from_arrays(offsets, values)
    expected = pa.array([values[:2], None, values[2:]],
                        type=list_type_factory(pa.binary()))

    assert result.equals(expected)

    # Another edge case
    offsets2 = [0, 2, None, 6]
    result = list_array_type.from_arrays(offsets2, values)
    expected = pa.array([values[:2], values[2:], None],
                        type=list_type_factory(pa.binary()))
    assert result.equals(expected)

    # raise on invalid array
    offsets = [1, 3, 10]
    values = np.arange(5)
    with pytest.raises(ValueError):
        list_array_type.from_arrays(offsets, values)

    # Non-monotonic offsets
    offsets = [0, 3, 2, 6]
    values = list(range(6))
    result = list_array_type.from_arrays(offsets, values)
    with pytest.raises(ValueError):
        result.validate(full=True)

    # mismatching type
    typ = list_type_factory(pa.binary())
    with pytest.raises(TypeError):
        list_array_type.from_arrays(offsets, values, type=typ)


@pytest.mark.numpy
def test_map_from_arrays():
    offsets_arr = np.array([0, 2, 5, 8], dtype='i4')
    offsets = pa.array(offsets_arr, type='int32')
    pykeys = [b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h']
    pyitems = list(range(len(pykeys)))
    pypairs = list(zip(pykeys, pyitems))
    pyentries = [pypairs[:2], pypairs[2:5], pypairs[5:8]]
    keys = pa.array(pykeys, type='binary')
    items = pa.array(pyitems, type='i4')

    result = pa.MapArray.from_arrays(offsets, keys, items)
    expected = pa.array(pyentries, type=pa.map_(pa.binary(), pa.int32()))

    assert result.equals(expected)

    # With nulls
    offsets = [0, None, 2, 6]
    pykeys = [b'a', b'b', b'c', b'd', b'e', b'f']
    pyitems = [1, 2, 3, None, 4, 5]
    pypairs = list(zip(pykeys, pyitems))
    pyentries = [pypairs[:2], None, pypairs[2:]]
    keys = pa.array(pykeys, type='binary')
    items = pa.array(pyitems, type='i4')

    result = pa.MapArray.from_arrays(offsets, keys, items)
    expected = pa.array(pyentries, type=pa.map_(pa.binary(), pa.int32()))

    assert result.equals(expected)

    # pass in the type explicitly
    result = pa.MapArray.from_arrays(offsets, keys, items, pa.map_(
        keys.type,
        items.type
    ))
    assert result.equals(expected)

    # pass in invalid types
    with pytest.raises(pa.ArrowTypeError, match='Expected map type, got string'):
        pa.MapArray.from_arrays(offsets, keys, items, pa.string())

    with pytest.raises(pa.ArrowTypeError, match='Mismatching map items type'):
        pa.MapArray.from_arrays(offsets, keys, items, pa.map_(
            keys.type,
            # Larger than the original i4
            pa.int64()
        ))

    # pass in null bitmap with type
    result = pa.MapArray.from_arrays([0, 2, 2, 6], keys, items, pa.map_(
        keys.type,
        items.type),
        mask=pa.array([False, True, False], type=pa.bool_())
    )
    assert result.null_count == 1
    assert result.equals(expected)

    # pass in null bitmap without the type
    result = pa.MapArray.from_arrays([0, 2, 2, 6], keys, items,
                                     mask=pa.array([False, True, False],
                                                   type=pa.bool_())
                                     )
    assert result.equals(expected)

    # pass in null bitmap with two nulls
    offsets = [0, None, None, 6]
    pyentries = [None, None, pypairs[2:]]

    result = pa.MapArray.from_arrays([0, 2, 2, 6], keys, items, pa.map_(
        keys.type,
        items.type),
        mask=pa.array([True, True, False], type=pa.bool_())
    )
    expected = pa.array(pyentries, type=pa.map_(pa.binary(), pa.int32()))
    assert result.null_count == 2
    assert result.equals(expected)

    # error if null bitmap and offsets with nulls passed
    msg1 = 'Ambiguous to specify both validity map and offsets with nulls'
    with pytest.raises(pa.ArrowInvalid, match=msg1):
        pa.MapArray.from_arrays(offsets, keys, items, pa.map_(
            keys.type,
            items.type),
            mask=pa.array([False, True, False], type=pa.bool_())
        )

    # error if null bitmap passed to sliced offset
    msg2 = 'Null bitmap with offsets slice not supported.'
    offsets = pa.array([0, 2, 2, 6], pa.int32())
    with pytest.raises(pa.ArrowNotImplementedError, match=msg2):
        pa.MapArray.from_arrays(offsets.slice(2), keys, items, pa.map_(
            keys.type,
            items.type),
            mask=pa.array([False, True, False], type=pa.bool_())
        )

    # check invalid usage
    offsets = [0, 1, 3, 5]
    keys = np.arange(5)
    items = np.arange(5)
    _ = pa.MapArray.from_arrays(offsets, keys, items)

    # raise on invalid offsets
    with pytest.raises(ValueError):
        pa.MapArray.from_arrays(offsets + [6], keys, items)

    # raise on length of keys != items
    with pytest.raises(ValueError):
        pa.MapArray.from_arrays(offsets, keys, np.concatenate([items, items]))

    # raise on keys with null
    keys_with_null = list(keys)[:-1] + [None]
    assert len(keys_with_null) == len(items)
    with pytest.raises(ValueError):
        pa.MapArray.from_arrays(offsets, keys_with_null, items)

    # Check if offset in offsets > 0
    offsets = pa.array(offsets, pa.int32())
    result = pa.MapArray.from_arrays(offsets.slice(1), keys, items)
    expected = pa.MapArray.from_arrays([1, 3, 5], keys, items)

    assert result.equals(expected)
    assert result.offset == 1
    assert expected.offset == 0

    offsets = pa.array([0, 0, 0, 0, 0, 0], pa.int32())
    result = pa.MapArray.from_arrays(
        offsets.slice(1),
        pa.array([], pa.string()),
        pa.array([], pa.string()),
    )
    expected = pa.MapArray.from_arrays(
        [0, 0, 0, 0, 0],
        pa.array([], pa.string()),
        pa.array([], pa.string()),
    )
    assert result.equals(expected)
    assert result.offset == 1
    assert expected.offset == 0


@pytest.mark.numpy
def test_cast_integers_safe():
    safe_cases = [
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='u4'), pa.uint16()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='u1'), pa.uint8()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='f8'), pa.float64())
    ]

    for case in safe_cases:
        _check_cast_case(case)

    unsafe_cases = [
        (np.array([50000], dtype='i4'), 'int32', 'int16'),
        (np.array([70000], dtype='i4'), 'int32', 'uint16'),
        (np.array([-1], dtype='i4'), 'int32', 'uint16'),
        (np.array([50000], dtype='u2'), 'uint16', 'int16')
    ]
    for in_data, in_type, out_type in unsafe_cases:
        in_arr = pa.array(in_data, type=in_type)

        with pytest.raises(pa.ArrowInvalid):
            in_arr.cast(out_type)


@pytest.mark.numpy
def test_cast_integers_unsafe():
    # We let NumPy do the unsafe casting.
    # Note that NEP50 in the NumPy spec no longer allows
    # the np.array() constructor to pass the dtype directly
    # if it results in an unsafe cast.
    unsafe_cases = [
        (np.array([50000], dtype='i4'), 'int32',
         np.array([50000]).astype(dtype='i2'), pa.int16()),
        (np.array([70000], dtype='i4'), 'int32',
         np.array([70000]).astype(dtype='u2'), pa.uint16()),
        (np.array([-1], dtype='i4'), 'int32',
         np.array([-1]).astype(dtype='u2'), pa.uint16()),
        (np.array([50000], dtype='u2'), pa.uint16(),
         np.array([50000]).astype(dtype='i2'), pa.int16())
    ]

    for case in unsafe_cases:
        _check_cast_case(case, safe=False)


@pytest.mark.numpy
def test_floating_point_truncate_safe():
    safe_cases = [
        (np.array([1.0, 2.0, 3.0], dtype='float32'), 'float32',
         np.array([1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([1.0, 2.0, 3.0], dtype='float64'), 'float64',
         np.array([1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([-10.0, 20.0, -30.0], dtype='float64'), 'float64',
         np.array([-10, 20, -30], dtype='i4'), pa.int32()),
    ]
    for case in safe_cases:
        _check_cast_case(case, safe=True)


@pytest.mark.numpy
def test_floating_point_truncate_unsafe():
    unsafe_cases = [
        (np.array([1.1, 2.2, 3.3], dtype='float32'), 'float32',
         np.array([1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([1.1, 2.2, 3.3], dtype='float64'), 'float64',
         np.array([1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([-10.1, 20.2, -30.3], dtype='float64'), 'float64',
         np.array([-10, 20, -30], dtype='i4'), pa.int32()),
    ]
    for case in unsafe_cases:
        # test safe casting raises
        with pytest.raises(pa.ArrowInvalid, match='truncated'):
            _check_cast_case(case, safe=True)

        # test unsafe casting truncates
        _check_cast_case(case, safe=False)


@pytest.mark.numpy
def test_decimal_to_int_value_out_of_bounds():
    out_of_bounds_cases = [
        (
            np.array([
                decimal.Decimal("1234567890123"),
                None,
                decimal.Decimal("-912345678901234")
            ]),
            pa.decimal128(32, 5),
            [1912276171, None, -135950322],
            pa.int32()
        ),
        (
            [decimal.Decimal("123456"), None, decimal.Decimal("-912345678")],
            pa.decimal128(32, 5),
            [-7616, None, -19022],
            pa.int16()
        ),
        (
            [decimal.Decimal("1234"), None, decimal.Decimal("-9123")],
            pa.decimal128(32, 5),
            [-46, None, 93],
            pa.int8()
        ),
    ]

    for case in out_of_bounds_cases:
        # test safe casting raises
        with pytest.raises(pa.ArrowInvalid,
                           match='Integer value out of bounds'):
            _check_cast_case(case)

        # XXX `safe=False` can be ignored when constructing an array
        # from a sequence of Python objects (ARROW-8567)
        _check_cast_case(case, safe=False, check_array_construction=False)


@pytest.mark.numpy
def test_safe_cast_nan_to_int_raises():
    arr = pa.array([np.nan, 1.])

    with pytest.raises(pa.ArrowInvalid, match='truncated'):
        arr.cast(pa.int64(), safe=True)


@pytest.mark.numpy
def test_cast_signed_to_unsigned():
    safe_cases = [
        (np.array([0, 1, 2, 3], dtype='i1'), pa.uint8(),
         np.array([0, 1, 2, 3], dtype='u1'), pa.uint8()),
        (np.array([0, 1, 2, 3], dtype='i2'), pa.uint16(),
         np.array([0, 1, 2, 3], dtype='u2'), pa.uint16())
    ]

    for case in safe_cases:
        _check_cast_case(case)


@pytest.mark.numpy
def test_cast_time32_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int32'),
                   type=pa.time32('s'))
    expected = pa.array([0, 1, 2], type='i4')

    result = arr.cast('i4')
    assert result.equals(expected)


@pytest.mark.numpy
def test_cast_time64_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.time64('us'))
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')
    assert result.equals(expected)


@pytest.mark.numpy
def test_cast_timestamp_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.timestamp('us'))
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')
    assert result.equals(expected)

@pytest.mark.numpy
def test_cast_duration_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.duration('us'))
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')
    assert result.equals(expected)


@pytest.mark.numpy
def test_cast_binary_to_utf8():
    binary_arr = pa.array([b'foo', b'bar', b'baz'], type=pa.binary())
    utf8_arr = binary_arr.cast(pa.utf8())
    expected = pa.array(['foo', 'bar', 'baz'], type=pa.utf8())

    assert utf8_arr.equals(expected)

    non_utf8_values = [('mañana').encode('utf-16-le')]
    non_utf8_binary = pa.array(non_utf8_values)
    assert non_utf8_binary.type == pa.binary()
    with pytest.raises(ValueError):
        non_utf8_binary.cast(pa.string())

    non_utf8_all_null = pa.array(non_utf8_values, mask=np.array([True]),
                                 type=pa.binary())
    # No error
    casted = non_utf8_all_null.cast(pa.string())
    assert casted.null_count == 1


@pytest.mark.numpy
def test_cast_date64_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.date64())
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')

    assert result.equals(expected)

@pytest.mark.numpy
@h.settings(suppress_health_check=(h.HealthCheck.too_slow,))
@h.given(
    past.arrays(
        past.all_types,
        size=st.integers(min_value=0, max_value=10)
    )
)
def test_pickling(pickle_module, arr):
    data = pickle_module.dumps(arr)
    restored = pickle_module.loads(data)
    assert arr.equals(restored)


@pytest.mark.numpy
def test_to_numpy_roundtrip():
    for narr in [
        np.arange(10, dtype=np.int64),
        np.arange(10, dtype=np.int32),
        np.arange(10, dtype=np.int16),
        np.arange(10, dtype=np.int8),
        np.arange(10, dtype=np.uint64),
        np.arange(10, dtype=np.uint32),
        np.arange(10, dtype=np.uint16),
        np.arange(10, dtype=np.uint8),
        np.arange(10, dtype=np.float64),
        np.arange(10, dtype=np.float32),
        np.arange(10, dtype=np.float16),
    ]:
        arr = pa.array(narr)
        assert narr.dtype == arr.to_numpy().dtype
        np.testing.assert_array_equal(narr, arr.to_numpy())
        np.testing.assert_array_equal(narr[:6], arr[:6].to_numpy())
        np.testing.assert_array_equal(narr[2:], arr[2:].to_numpy())
        np.testing.assert_array_equal(narr[2:6], arr[2:6].to_numpy())


@pytest.mark.numpy
def test_array_uint64_from_py_over_range():
    arr = pa.array([2 ** 63], type=pa.uint64())
    expected = pa.array(np.array([2 ** 63], dtype='u8'))
    assert arr.equals(expected)


@pytest.mark.numpy
def test_array_conversions_no_sentinel_values():
    arr = np.array([1, 2, 3, 4], dtype='int8')
    refcount = sys.getrefcount(arr)
    arr2 = pa.array(arr)  # noqa
    assert sys.getrefcount(arr) == (refcount + 1)

    assert arr2.type == 'int8'

    for ty in ['float16', 'float32', 'float64']:
        arr3 = pa.array(np.array([1, np.nan, 2, 3, np.nan, 4], dtype=ty),
                        type=ty)
        assert arr3.type == ty
        assert arr3.null_count == 0


@pytest.mark.numpy
def test_binary_string_pandas_null_sentinels():
    # ARROW-6227
    def _check_case(ty):
        arr = pa.array(['string', np.nan], type=ty, from_pandas=True)
        expected = pa.array(['string', None], type=ty)
        assert arr.equals(expected)
    _check_case('binary')
    _check_case('utf8')


@pytest.mark.numpy
def test_pandas_null_sentinels_raise_error():
    # ARROW-6227
    cases = [
        ([None, np.nan], 'null'),
        (['string', np.nan], 'binary'),
        (['string', np.nan], 'utf8'),
        (['string', np.nan], 'large_binary'),
        (['string', np.nan], 'large_utf8'),
        ([b'string', np.nan], pa.binary(6)),
        ([True, np.nan], pa.bool_()),
        ([decimal.Decimal('0'), np.nan], pa.decimal128(12, 2)),
        ([0, np.nan], pa.date32()),
        ([0, np.nan], pa.date32()),
        ([0, np.nan], pa.date64()),
        ([0, np.nan], pa.time32('s')),
        ([0, np.nan], pa.time64('us')),
        ([0, np.nan], pa.timestamp('us')),
        ([0, np.nan], pa.duration('us')),
    ]
    for case, ty in cases:
        # Both types of exceptions are raised. May want to clean that up
        with pytest.raises((ValueError, TypeError)):
            pa.array(case, type=ty)

        # from_pandas option suppresses failure
        result = pa.array(case, type=ty, from_pandas=True)
        assert result.null_count == (1 if ty != 'null' else 2)


@pytest.mark.numpy
def test_array_roundtrip_from_numpy_datetimeD():
    arr = np.array([None, datetime.date(2017, 4, 4)], dtype='datetime64[D]')

    result = pa.array(arr)
    expected = pa.array([None, datetime.date(2017, 4, 4)], type=pa.date32())
    assert result.equals(expected)
    result = result.to_numpy(zero_copy_only=False)
    np.testing.assert_array_equal(result, arr)
    assert result.dtype == arr.dtype


def test_array_from_naive_datetimes():
    arr = pa.array([
        None,
        datetime.datetime(2017, 4, 4, 12, 11, 10),
        datetime.datetime(2018, 1, 1, 0, 2, 0)
    ])
    assert arr.type == pa.timestamp('us', tz=None)


@pytest.mark.numpy
@pytest.mark.parametrize(('dtype', 'type'), [
    ('datetime64[s]', pa.timestamp('s')),
    ('datetime64[ms]', pa.timestamp('ms')),
    ('datetime64[us]', pa.timestamp('us')),
    ('datetime64[ns]', pa.timestamp('ns'))
])
def test_array_from_numpy_datetime(dtype, type):
    data = [
        None,
        datetime.datetime(2017, 4, 4, 12, 11, 10),
        datetime.datetime(2018, 1, 1, 0, 2, 0)
    ]

    # from numpy array
    arr = pa.array(np.array(data, dtype=dtype))
    expected = pa.array(data, type=type)
    assert arr.equals(expected)

    # from list of numpy scalars
    arr = pa.array(list(np.array(data, dtype=dtype)))
    assert arr.equals(expected)


@pytest.mark.numpy
def test_array_from_different_numpy_datetime_units_raises():
    data = [
        None,
        datetime.datetime(2017, 4, 4, 12, 11, 10),
        datetime.datetime(2018, 1, 1, 0, 2, 0)
    ]
    s = np.array(data, dtype='datetime64[s]')
    ms = np.array(data, dtype='datetime64[ms]')
    data = list(s[:2]) + list(ms[2:])

    with pytest.raises(pa.ArrowInvalid,
                       match="Cannot mix NumPy datetime64 units s and ms"):
        pa.array(data)


@pytest.mark.numpy
@pytest.mark.parametrize('unit', [
    'Y',  # year
    'M',  # month
    'W',  # week
    'h',  # hour
    'm',  # minute
    'ps',  # picosecond
    'fs',  # femtosecond
    'as',  # attosecond
])
def test_array_from_unsupported_numpy_datetime_unit_names(unit):
    s_data = [np.datetime64('2020-01-01', 's')]
    unsupported_data = [np.datetime64('2020', unit)]

    # Mix supported unit (s) with unsupported unit
    data = s_data + unsupported_data

    with pytest.raises(pa.ArrowInvalid,
                       match=f"Cannot mix NumPy datetime64 units s and {unit}"):
        pa.array(data)


@pytest.mark.numpy
@pytest.mark.parametrize('unit', ['ns', 'us', 'ms', 's'])
def test_array_from_list_of_timestamps(unit):
    n = np.datetime64('NaT', unit)
    x = np.datetime64('2017-01-01 01:01:01.111111111', unit)
    y = np.datetime64('2018-11-22 12:24:48.111111111', unit)

    a1 = pa.array([n, x, y])
    a2 = pa.array([n, x, y], type=pa.timestamp(unit))

    assert a1.type == a2.type
    assert a1.type.unit == unit
    assert a1[0] == a2[0]


@pytest.mark.numpy
def test_array_from_timestamp_with_generic_unit():
    n = np.datetime64('NaT')
    x = np.datetime64('2017-01-01 01:01:01.111111111')
    y = np.datetime64('2018-11-22 12:24:48.111111111')

    with pytest.raises(pa.ArrowInvalid,
                       match='Cannot mix NumPy datetime64 units'):
        pa.array([n, x, y])


@pytest.mark.numpy
@pytest.mark.parametrize(('dtype', 'type'), [
    ('timedelta64[s]', pa.duration('s')),
    ('timedelta64[ms]', pa.duration('ms')),
    ('timedelta64[us]', pa.duration('us')),
    ('timedelta64[ns]', pa.duration('ns'))
])
def test_array_from_numpy_timedelta(dtype, type):
    data = [
        None,
        datetime.timedelta(1),
        datetime.timedelta(0, 1)
    ]

    # from numpy array
    np_arr = np.array(data, dtype=dtype)
    arr = pa.array(np_arr)
    assert isinstance(arr, pa.DurationArray)
    assert arr.type == type
    expected = pa.array(data, type=type)
    assert arr.equals(expected)
    assert arr.to_pylist() == data

    # from list of numpy scalars
    arr = pa.array(list(np.array(data, dtype=dtype)))
    assert arr.equals(expected)
    assert arr.to_pylist() == data


@pytest.mark.numpy
def test_array_from_numpy_timedelta_incorrect_unit():
    # generic (no unit)
    td = np.timedelta64(1)

    for data in [[td], np.array([td])]:
        with pytest.raises(NotImplementedError):
            pa.array(data)

    # unsupported unit
    td = np.timedelta64(1, 'M')
    for data in [[td], np.array([td])]:
        with pytest.raises(NotImplementedError):
            pa.array(data)


@pytest.mark.numpy
@pytest.mark.parametrize('binary_type', [
    None,
    pa.binary(),
    pa.large_binary(),
    pa.binary_view()])
def test_array_from_numpy_ascii(binary_type):
    # Default when no type is specified should be binary
    expected_type = binary_type or pa.binary()

    arr = np.array(['abcde', 'abc', ''], dtype='|S5')

    arrow_arr = pa.array(arr, binary_type)
    assert arrow_arr.type == expected_type
    expected = pa.array(['abcde', 'abc', ''], type=expected_type)
    assert arrow_arr.equals(expected)

    mask = np.array([False, True, False])
    arrow_arr = pa.array(arr, binary_type, mask=mask)
    expected = pa.array(['abcde', None, ''], type=expected_type)
    assert arrow_arr.equals(expected)

    # Strided variant
    arr = np.array(['abcde', 'abc', ''] * 5, dtype='|S5')[::2]
    mask = np.array([False, True, False] * 5)[::2]
    arrow_arr = pa.array(arr, binary_type, mask=mask)

    expected = pa.array(['abcde', '', None, 'abcde', '', None, 'abcde', ''],
                        type=expected_type)
    assert arrow_arr.equals(expected)

    # 0 itemsize
    arr = np.array(['', '', ''], dtype='|S0')
    arrow_arr = pa.array(arr, binary_type)
    expected = pa.array(['', '', ''], type=expected_type)
    assert arrow_arr.equals(expected)


@pytest.mark.numpy
@pytest.mark.parametrize('string_type', [
    None,
    pa.utf8(),
    pa.large_utf8(),
    pa.string_view()])
def test_array_from_numpy_unicode(string_type):
    # Default when no type is specified should be utf8
    expected_type = string_type or pa.utf8()

    dtypes = ['<U5', '>U5']

    for dtype in dtypes:
        arr = np.array(['abcde', 'abc', ''], dtype=dtype)

        arrow_arr = pa.array(arr, string_type)
        assert arrow_arr.type == expected_type
        expected = pa.array(['abcde', 'abc', ''], type=expected_type)
        assert arrow_arr.equals(expected)

        mask = np.array([False, True, False])
        arrow_arr = pa.array(arr, string_type, mask=mask)
        expected = pa.array(['abcde', None, ''], type=expected_type)
        assert arrow_arr.equals(expected)

        # Strided variant
        arr = np.array(['abcde', 'abc', ''] * 5, dtype=dtype)[::2]
        mask = np.array([False, True, False] * 5)[::2]
        arrow_arr = pa.array(arr, string_type, mask=mask)

        expected = pa.array(['abcde', '', None, 'abcde', '', None,
                             'abcde', ''], type=expected_type)
        assert arrow_arr.equals(expected)

    # 0 itemsize
    arr = np.array(['', '', ''], dtype='<U0')
    arrow_arr = pa.array(arr, string_type)
    expected = pa.array(['', '', ''], type=expected_type)
    assert arrow_arr.equals(expected)


@pytest.mark.numpy
def test_array_string_from_non_string():
    # ARROW-5682 - when converting to string raise on non string-like dtype
    with pytest.raises(TypeError):
        pa.array(np.array([1, 2, 3]), type=pa.string())


@pytest.mark.numpy
def test_array_string_from_all_null():
    # ARROW-5682
    vals = np.array([None, None], dtype=object)
    arr = pa.array(vals, type=pa.string())
    assert arr.null_count == 2

    vals = np.array([np.nan, np.nan], dtype='float64')
    # by default raises, but accept as all-null when from_pandas=True
    with pytest.raises(TypeError):
        pa.array(vals, type=pa.string())
    arr = pa.array(vals, type=pa.string(), from_pandas=True)
    assert arr.null_count == 2


@pytest.mark.numpy
def test_array_from_masked():
    ma = np.ma.array([1, 2, 3, 4], dtype='int64',
                     mask=[False, False, True, False])
    result = pa.array(ma)
    expected = pa.array([1, 2, None, 4], type='int64')
    assert expected.equals(result)

    with pytest.raises(ValueError, match="Cannot pass a numpy masked array"):
        pa.array(ma, mask=np.array([True, False, False, False]))


@pytest.mark.numpy
def test_array_from_shrunken_masked():
    ma = np.ma.array([0], dtype='int64')
    result = pa.array(ma)
    expected = pa.array([0], type='int64')
    assert expected.equals(result)


@pytest.mark.numpy
def test_array_from_invalid_dim_raises():
    msg = "only handle 1-dimensional arrays"
    arr2d = np.array([[1, 2, 3], [4, 5, 6]])
    with pytest.raises(ValueError, match=msg):
        pa.array(arr2d)

    arr0d = np.array(0)
    with pytest.raises(ValueError, match=msg):
        pa.array(arr0d)


@pytest.mark.numpy
def test_array_from_strided_bool():
    # ARROW-6325
    arr = np.ones((3, 2), dtype=bool)
    result = pa.array(arr[:, 0])
    expected = pa.array([True, True, True])
    assert result.equals(expected)
    result = pa.array(arr[0, :])
    expected = pa.array([True, True])
    assert result.equals(expected)


@pytest.mark.numpy
def test_array_from_strided():
    pydata = [
        ([b"ab", b"cd", b"ef"], (pa.binary(), pa.binary(2))),
        ([1, 2, 3], (pa.int8(), pa.int16(), pa.int32(), pa.int64())),
        ([1.0, 2.0, 3.0], (pa.float32(), pa.float64())),
        (["ab", "cd", "ef"], (pa.utf8(), ))
    ]

    for values, dtypes in pydata:
        nparray = np.array(values)
        for patype in dtypes:
            for mask in (None, np.array([False, False])):
                arrow_array = pa.array(nparray[::2], patype,
                                       mask=mask)
                assert values[::2] == arrow_array.to_pylist()


@pytest.mark.numpy
def test_buffers_primitive():
    a = pa.array([1, 2, None, 4], type=pa.int16())
    buffers = a.buffers()
    assert len(buffers) == 2
    null_bitmap = buffers[0].to_pybytes()
    assert 1 <= len(null_bitmap) <= 64  # XXX this is varying
    assert bytearray(null_bitmap)[0] == 0b00001011

    # Slicing does not affect the buffers but the offset
    a_sliced = a[1:]
    buffers = a_sliced.buffers()
    a_sliced.offset == 1
    assert len(buffers) == 2
    null_bitmap = buffers[0].to_pybytes()
    assert 1 <= len(null_bitmap) <= 64  # XXX this is varying
    assert bytearray(null_bitmap)[0] == 0b00001011

    assert struct.unpack('hhxxh', buffers[1].to_pybytes()) == (1, 2, 4)

    a = pa.array(np.int8([4, 5, 6]))
    buffers = a.buffers()
    assert len(buffers) == 2
    # No null bitmap from Numpy int array
    assert buffers[0] is None
    assert struct.unpack('3b', buffers[1].to_pybytes()) == (4, 5, 6)

    a = pa.array([b'foo!', None, b'bar!!'])
    buffers = a.buffers()
    assert len(buffers) == 3
    null_bitmap = buffers[0].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000101
    offsets = buffers[1].to_pybytes()
    assert struct.unpack('4i', offsets) == (0, 4, 4, 9)
    values = buffers[2].to_pybytes()
    assert values == b'foo!bar!!'


@pytest.mark.numpy
def test_total_buffer_size():
    a = pa.array(np.array([4, 5, 6], dtype='int64'))
    assert a.nbytes == 8 * 3
    assert a.get_total_buffer_size() == 8 * 3
    assert sys.getsizeof(a) >= object.__sizeof__(a) + a.nbytes
    a = pa.array([1, None, 3], type='int64')
    assert a.nbytes == 8*3 + 1
    assert a.get_total_buffer_size() == 8*3 + 1
    assert sys.getsizeof(a) >= object.__sizeof__(a) + a.nbytes
    a = pa.array([[1, 2], None, [3, None, 4, 5]], type=pa.list_(pa.int64()))
    assert a.nbytes == 62
    assert a.get_total_buffer_size() == 1 + 4 * 4 + 1 + 6 * 8
    assert sys.getsizeof(a) >= object.__sizeof__(a) + a.nbytes
    a = pa.array([[[5, 6, 7]], [[9, 10]]], type=pa.list_(pa.list_(pa.int8())))
    assert a.get_total_buffer_size() == (4 * 3) + (4 * 3) + (1 * 5)
    assert a.nbytes == 21
    a = pa.array([[[1, 2], [3, 4]], [[5, 6, 7], None, [8]], [[9, 10]]],
                 type=pa.list_(pa.list_(pa.int8())))
    a1 = a.slice(1, 2)
    assert a1.nbytes == (4 * 2) + 1 + (4 * 4) + (1 * 6)
    assert a1.get_total_buffer_size() == (4 * 4) + 1 + (4 * 7) + (1 * 10)



@pytest.mark.numpy
def test_array_from_numpy_str_utf8():
    # ARROW-3890 -- in Python 3, NPY_UNICODE arrays are produced, but in Python
    # 2 they are NPY_STRING (binary), so we must do UTF-8 validation
    vec = np.array(["toto", "tata"])
    vec2 = np.array(["toto", "tata"], dtype=object)

    arr = pa.array(vec, pa.string())
    arr2 = pa.array(vec2, pa.string())
    expected = pa.array(["toto", "tata"])
    assert arr.equals(expected)
    assert arr2.equals(expected)

    # with mask, separate code path
    mask = np.array([False, False], dtype=bool)
    arr = pa.array(vec, pa.string(), mask=mask)
    assert arr.equals(expected)

    # UTF8 validation failures
    vec = np.array([('mañana').encode('utf-16-le')])
    with pytest.raises(ValueError):
        pa.array(vec, pa.string())

    with pytest.raises(ValueError):
        pa.array(vec, pa.string(), mask=np.array([False]))


@pytest.mark.numpy
@pytest.mark.slow
@pytest.mark.large_memory
@pytest.mark.parametrize('large_types', [False, True])
def test_numpy_binary_overflow_to_chunked(large_types):
    # ARROW-3762, ARROW-5966, GH-35289

    # 2^31 + 1 bytes
    values = [b'x']
    unicode_values = ['x']

    # Make 10 unique 1MB strings then repeat then 2048 times
    unique_strings = {
        i: b'x' * ((1 << 20) - 1) + str(i % 10).encode('utf8')
        for i in range(10)
    }
    unicode_unique_strings = {i: x.decode('utf8')
                              for i, x in unique_strings.items()}
    values += [unique_strings[i % 10] for i in range(1 << 11)]
    unicode_values += [unicode_unique_strings[i % 10]
                       for i in range(1 << 11)]

    binary_type = pa.large_binary() if large_types else pa.binary()
    string_type = pa.large_utf8() if large_types else pa.utf8()
    for case, ex_type in [(values, binary_type),
                          (unicode_values, string_type)]:
        arr = np.array(case)
        arrow_arr = pa.array(arr, ex_type)
        arr = None

        assert arrow_arr.type == ex_type
        if large_types:
            # Large types shouldn't be chunked
            assert isinstance(arrow_arr, pa.Array)

            for i in range(len(arrow_arr)):
                val = arrow_arr[i]
                assert val.as_py() == case[i]
        else:
            assert isinstance(arrow_arr, pa.ChunkedArray)

            # Split up into 16MB chunks. 128 * 16 = 2048, so 129
            assert arrow_arr.num_chunks == 129

            value_index = 0
            for i in range(arrow_arr.num_chunks):
                chunk = arrow_arr.chunk(i)
                for val in chunk:
                    assert val.as_py() == case[value_index]
                    value_index += 1


@pytest.mark.numpy
def test_infer_type_masked():
    # ARROW-5208
    ty = pa.infer_type(['foo', 'bar', None, 2],
                       mask=[False, False, False, True])
    assert ty == pa.utf8()

    # all masked
    ty = pa.infer_type(['foo', 'bar', None, 2],
                       mask=np.array([True, True, True, True]))
    assert ty == pa.null()

    # length 0
    assert pa.infer_type([], mask=[]) == pa.null()


@pytest.mark.numpy
def test_array_masked():
    # ARROW-5208
    arr = pa.array([4, None, 4, 3.],
                   mask=np.array([False, True, False, True]))
    assert arr.type == pa.int64()

    # ndarray dtype=object argument
    arr = pa.array(np.array([4, None, 4, 3.], dtype="O"),
                   mask=np.array([False, True, False, True]))
    assert arr.type == pa.int64()


@pytest.mark.numpy
def test_array_supported_masks():
    # ARROW-13883
    arr = pa.array([4, None, 4, 3.],
                   mask=np.array([False, True, False, True]))
    assert arr.to_pylist() == [4, None, 4, None]

    arr = pa.array([4, None, 4, 3],
                   mask=pa.array([False, True, False, True]))
    assert arr.to_pylist() == [4, None, 4, None]

    arr = pa.array([4, None, 4, 3],
                   mask=[False, True, False, True])
    assert arr.to_pylist() == [4, None, 4, None]

    arr = pa.array([4, 3, None, 3],
                   mask=[False, True, False, True])
    assert arr.to_pylist() == [4, None, None, None]

    # Non boolean values
    with pytest.raises(pa.ArrowTypeError):
        arr = pa.array([4, None, 4, 3],
                       mask=pa.array([1.0, 2.0, 3.0, 4.0]))

    with pytest.raises(pa.ArrowTypeError):
        arr = pa.array([4, None, 4, 3],
                       mask=[1.0, 2.0, 3.0, 4.0])

    with pytest.raises(pa.ArrowTypeError):
        arr = pa.array([4, None, 4, 3],
                       mask=np.array([1.0, 2.0, 3.0, 4.0]))

    with pytest.raises(pa.ArrowTypeError):
        arr = pa.array([4, None, 4, 3],
                       mask=pa.array([False, True, False, True],
                                     mask=pa.array([True, True, True, True])))

    with pytest.raises(pa.ArrowTypeError):
        arr = pa.array([4, None, 4, 3],
                       mask=pa.array([False, None, False, True]))

    # Numpy arrays only accepts numpy masks
    with pytest.raises(TypeError):
        arr = pa.array(np.array([4, None, 4, 3.]),
                       mask=[True, False, True, False])

    with pytest.raises(TypeError):
        arr = pa.array(np.array([4, None, 4, 3.]),
                       mask=pa.array([True, False, True, False]))


@pytest.mark.numpy
def test_binary_array_masked():
    # ARROW-12431
    masked_basic = pa.array([b'\x05'], type=pa.binary(1),
                            mask=np.array([False]))
    assert [b'\x05'] == masked_basic.to_pylist()

    # Fixed Length Binary
    masked = pa.array(np.array([b'\x05']), type=pa.binary(1),
                      mask=np.array([False]))
    assert [b'\x05'] == masked.to_pylist()

    masked_nulls = pa.array(np.array([b'\x05']), type=pa.binary(1),
                            mask=np.array([True]))
    assert [None] == masked_nulls.to_pylist()

    # Variable Length Binary
    masked = pa.array(np.array([b'\x05']), type=pa.binary(),
                      mask=np.array([False]))
    assert [b'\x05'] == masked.to_pylist()

    masked_nulls = pa.array(np.array([b'\x05']), type=pa.binary(),
                            mask=np.array([True]))
    assert [None] == masked_nulls.to_pylist()

    # Fixed Length Binary, copy
    npa = np.array([b'aaa', b'bbb', b'ccc']*10)
    arrow_array = pa.array(npa, type=pa.binary(3),
                           mask=np.array([False, False, False]*10))
    npa[npa == b"bbb"] = b"XXX"
    assert ([b'aaa', b'bbb', b'ccc']*10) == arrow_array.to_pylist()


@pytest.mark.numpy
def test_binary_array_strided():
    # Masked
    nparray = np.array([b"ab", b"cd", b"ef"])
    arrow_array = pa.array(nparray[::2], pa.binary(2),
                           mask=np.array([False, False]))
    assert [b"ab", b"ef"] == arrow_array.to_pylist()

    # Unmasked
    nparray = np.array([b"ab", b"cd", b"ef"])
    arrow_array = pa.array(nparray[::2], pa.binary(2))
    assert [b"ab", b"ef"] == arrow_array.to_pylist()


@pytest.mark.numpy
def test_array_invalid_mask_raises():
    # ARROW-10742
    cases = [
        ([1, 2], np.array([False, False], dtype="O"),
         TypeError, "must be boolean dtype"),

        ([1, 2], np.array([[False], [False]]),
         pa.ArrowInvalid, "must be 1D array"),

        ([1, 2, 3], np.array([False, False]),
         pa.ArrowInvalid, "different length"),

        (np.array([1, 2]), np.array([False, False], dtype="O"),
         TypeError, "must be boolean dtype"),

        (np.array([1, 2]), np.array([[False], [False]]),
         ValueError, "must be 1D array"),

        (np.array([1, 2, 3]), np.array([False, False]),
         ValueError, "different length"),
    ]
    for obj, mask, ex, msg in cases:
        with pytest.raises(ex, match=msg):
            pa.array(obj, mask=mask)


@pytest.mark.numpy
def test_numpy_array_protocol():
    # test the __array__ method on pyarrow.Array
    arr = pa.array([1, 2, 3])
    result = np.asarray(arr)
    expected = np.array([1, 2, 3], dtype="int64")
    np.testing.assert_array_equal(result, expected)

    # this should not raise a deprecation warning with numpy 2.0+
    result = np.array(arr, copy=False)
    np.testing.assert_array_equal(result, expected)

    result = np.array(arr, dtype="int64", copy=False)
    np.testing.assert_array_equal(result, expected)

    # no zero-copy is possible
    arr = pa.array([1, 2, None])
    expected = np.array([1, 2, np.nan], dtype="float64")
    result = np.asarray(arr)
    np.testing.assert_array_equal(result, expected)

    if Version(np.__version__) < Version("2.0.0.dev0"):
        # copy keyword is not strict and not passed down to __array__
        result = np.array(arr, copy=False)
        np.testing.assert_array_equal(result, expected)

        result = np.array(arr, dtype="float64", copy=False)
        np.testing.assert_array_equal(result, expected)
    else:
        # starting with numpy 2.0, the copy=False keyword is assumed to be strict
        with pytest.raises(ValueError, match="Unable to avoid a copy"):
            np.array(arr, copy=False)

        arr = pa.array([1, 2, 3])
        with pytest.raises(ValueError):
            np.array(arr, dtype="float64", copy=False)

    # copy=True -> not yet passed by numpy, so we have to call this directly to test
    arr = pa.array([1, 2, 3])
    result = arr.__array__(copy=True)
    assert result.flags.writeable

    arr = pa.array([1, 2, 3])
    result = arr.__array__(dtype=np.dtype("float64"), copy=True)
    assert result.dtype == "float64"


@pytest.mark.numpy
def test_array_protocol():

    class MyArray:
        def __init__(self, data):
            self.data = data

        def __arrow_array__(self, type=None):
            return pa.array(self.data, type=type)

    arr = MyArray(np.array([1, 2, 3], dtype='int64'))
    result = pa.array(arr)
    expected = pa.array([1, 2, 3], type=pa.int64())
    assert result.equals(expected)
    result = pa.array(arr, type=pa.int64())
    expected = pa.array([1, 2, 3], type=pa.int64())
    assert result.equals(expected)
    result = pa.array(arr, type=pa.float64())
    expected = pa.array([1, 2, 3], type=pa.float64())
    assert result.equals(expected)

    # raise error when passing size or mask keywords
    with pytest.raises(ValueError):
        pa.array(arr, mask=np.array([True, False, True]))
    with pytest.raises(ValueError):
        pa.array(arr, size=3)

    # ensure the return value is an Array
    class MyArrayInvalid:
        def __init__(self, data):
            self.data = data

        def __arrow_array__(self, type=None):
            return np.array(self.data)

    arr = MyArrayInvalid(np.array([1, 2, 3], dtype='int64'))
    with pytest.raises(TypeError):
        pa.array(arr)

    # ARROW-7066 - allow ChunkedArray output
    # GH-33727 - if num_chunks=1 return Array
    class MyArray2:
        def __init__(self, data):
            self.data = data

        def __arrow_array__(self, type=None):
            return pa.chunked_array([self.data], type=type)

    arr = MyArray2(np.array([1, 2, 3], dtype='int64'))
    result = pa.array(arr)
    expected = pa.array([1, 2, 3], type=pa.int64())
    assert result.equals(expected)

    class MyArray3:
        def __init__(self, data1, data2):
            self.data1 = data1
            self.data2 = data2

        def __arrow_array__(self, type=None):
            return pa.chunked_array([self.data1, self.data2], type=type)

    np_arr = np.array([1, 2, 3], dtype='int64')
    arr = MyArray3(np_arr, np_arr)
    result = pa.array(arr)
    expected = pa.chunked_array([[1, 2, 3], [1, 2, 3]], type=pa.int64())
    assert result.equals(expected)


@pytest.mark.numpy
def test_run_end_encoded_from_array_with_type():
    run_ends = [1, 3, 6]
    values = [1, 2, 3]
    ree_type = pa.run_end_encoded(pa.int32(), pa.int64())
    expected = pa.RunEndEncodedArray.from_arrays(run_ends, values,
                                                 ree_type)

    arr = [1, 2, 2, 3, 3, 3]
    result = pa.array(arr, type=ree_type)
    assert result.equals(expected)
    result = pa.array(np.array(arr), type=ree_type)
    assert result.equals(expected)

    ree_type_2 = pa.run_end_encoded(pa.int16(), pa.float32())
    result = pa.array(arr, type=ree_type_2)
    assert not result.equals(expected)
    expected_2 = pa.RunEndEncodedArray.from_arrays(run_ends, values,
                                                   ree_type_2)
    assert result.equals(expected_2)

    run_ends = [1, 3, 5, 6]
    values = [1, 2, 3, None]
    expected = pa.RunEndEncodedArray.from_arrays(run_ends, values,
                                                 ree_type)

    arr = [1, 2, 2, 3, 3, None]
    result = pa.array(arr, type=ree_type)
    assert result.equals(expected)

    run_ends = [1, 3, 4, 5, 6]
    values = [1, 2, None, 3, None]
    expected = pa.RunEndEncodedArray.from_arrays(run_ends, values,
                                                 ree_type)

    mask = pa.array([False, False, False, True, False, True])
    result = pa.array(arr, type=ree_type, mask=mask)
    assert result.equals(expected)


@pytest.mark.numpy
def test_run_end_encoded_to_numpy():
    arr = [1, 2, 2, 3, 3, 3]
    ree_array = pa.array(arr, pa.run_end_encoded(pa.int32(), pa.int64()))
    expected = np.array(arr)

    np.testing.assert_array_equal(ree_array.to_numpy(zero_copy_only=False), expected)

    with pytest.raises(pa.ArrowInvalid):
        ree_array.to_numpy()


@pytest.mark.numpy
@pytest.mark.parametrize('numpy_native_dtype', ['u2', 'i4', 'f8'])
def test_swapped_byte_order_fails(numpy_native_dtype):
    # ARROW-39129

    numpy_swapped_dtype = np.dtype(numpy_native_dtype).newbyteorder()
    np_arr = np.arange(10, dtype=numpy_swapped_dtype)

    # Primitive type array, type is inferred from the numpy array
    with pytest.raises(pa.ArrowNotImplementedError):
        pa.array(np_arr)

    # Primitive type array, type is explicitly provided
    with pytest.raises(pa.ArrowNotImplementedError):
        pa.array(np_arr, type=pa.float64())

    # List type array
    with pytest.raises(pa.ArrowNotImplementedError):
        pa.array([np_arr])

    # Struct type array
    with pytest.raises(pa.ArrowNotImplementedError):
        pa.StructArray.from_arrays([np_arr], names=['a'])