import math
import pyarrow as pa
import pyarrow.compute as pc
import pytest

def test_cast_coerce():
    arr = pa.array(["1.1", "2.2", "abc", "4.4"])
    
    # Should produce null for "abc"
    casted = pc.cast(arr, pa.float64(), errors='coerce')
    expected = pa.array([1.1, 2.2, None, 4.4])
    assert casted.equals(expected)

def test_cast_coerce_issue_example():
    """Test the exact example from issue #48972"""
    arr = pa.array(["1.2", "3", "10-20", None, "nan", ""])
    
    # Should not raise, but produce nulls for invalid values
    out = pc.cast(arr, pa.float64(), safe=False, errors='coerce')
    
    # Expected: [1.2, 3, null, null, nan, null]
    # Note: "nan" should be cast to NaN, not null
    assert out[0].as_py() == 1.2
    assert out[1].as_py() == 3.0
    assert out[2].is_valid == False  # "10-20" cannot be cast
    assert out[3].is_valid == False  # None stays null
    assert math.isnan(out[4].as_py())  # "nan" becomes NaN
    assert out[5].is_valid == False  # "" cannot be cast

def test_is_castable():
    arr = pa.array(["1.1", "2.2", "abc", "4.4"])
    
    # Should be false for "abc"
    castable = pc.is_castable(arr, pa.float64())
    expected = pa.array([True, True, False, True])
    assert castable.equals(expected)

    # Boolean test
    arr_bool = pa.array(["true", "false", "maybe", None])
    castable_bool = pc.is_castable(arr_bool, pa.bool_())
    expected_bool = pa.array([True, True, False, True])
    assert castable_bool.equals(expected_bool)

def test_is_castable_issue_example():
    """Test is_castable with the exact example from issue #48972"""
    arr = pa.array(["1.2", "3", "10-20", None, "nan", ""])
    
    castable = pc.is_castable(arr, pa.float64())
    # Expected: [True, True, False, True, True, False]
    # "1.2", "3", "nan" are castable, None is castable (nulls are considered castable),
    # "10-20" and "" are not castable
    expected = pa.array([True, True, False, True, True, False])
    assert castable.equals(expected)

def test_cast_coerce_temporal():
    arr = pa.array(["2020-01-01", "invalid-date", "2021-02-02"])
    
    casted = pc.cast(arr, pa.date32(), errors='coerce')
    expected = pa.array([pa.scalar("2020-01-01").cast(pa.date32()), None, pa.scalar("2021-02-02").cast(pa.date32())])
    assert casted.equals(expected)

def test_cast_with_options_and_errors():
    """Test that errors parameter works even when options is provided"""
    arr = pa.array(["1.1", "abc", "2.2"])
    options = pc.CastOptions.safe(pa.float64())
    
    # Should respect errors='coerce' even when options is provided
    casted = pc.cast(arr, options=options, errors='coerce')
    expected = pa.array([1.1, None, 2.2])
    assert casted.equals(expected)

def test_cast_instance_method():
    """Test that errors parameter works with instance methods"""
    arr = pa.array(["1.1", "abc", "2.2"])
    
    # Test array.cast() instance method
    casted = arr.cast(pa.float64(), errors='coerce')
    expected = pa.array([1.1, None, 2.2])
    assert casted.equals(expected)
    
    # Test chunked_array.cast() instance method
    chunked = pa.chunked_array([["1.1", "abc"], ["2.2"]])
    casted_chunked = chunked.cast(pa.float64(), errors='coerce')
    assert casted_chunked[0].as_py() == 1.1
    assert casted_chunked[1].is_valid == False  # "abc" becomes null
    assert casted_chunked[2].as_py() == 2.2

def test_cast_errors_validation():
    """Test error handling for invalid errors parameter"""
    arr = pa.array(["1.1", "2.2"])
    
    # Invalid errors value should raise ValueError
    with pytest.raises(ValueError, match="errors must be either 'raise' or 'coerce'"):
        pc.cast(arr, pa.float64(), errors='invalid')
    
    # Both target_type and options should raise ValueError
    options = pc.CastOptions.safe(pa.float64())
    with pytest.raises(ValueError, match="Must either pass"):
        pc.cast(arr, pa.float64(), options=options)
    
    # Neither target_type nor options should raise ValueError
    with pytest.raises(ValueError, match="Must provide either"):
        pc.cast(arr)

def test_cast_errors_with_unsafe():
    """Test that errors='coerce' works with safe=False (unsafe casting)"""
    arr = pa.array(["1.2", "3", "10-20", None, "nan", ""])
    
    # Should work with unsafe casting and coerce
    out = pc.cast(arr, pa.float64(), safe=False, errors='coerce')
    assert out[0].as_py() == 1.2
    assert out[1].as_py() == 3.0
    assert out[2].is_valid == False  # "10-20" cannot be cast
    assert out[3].is_valid == False  # None stays null
    assert math.isnan(out[4].as_py())  # "nan" becomes NaN
    assert out[5].is_valid == False  # "" cannot be cast
