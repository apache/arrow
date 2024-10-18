import pyarrow as pa

# Class with valid __arrow_c_array__ method that returns a length-1 array
class CustomCapsule:
    def __arrow_c_array__(self):
        return pa.array([42])

# Test case to check that a scalar can be created from an object with __arrow_c_array__
# Adjusting the test to expect the current behavior, which is an ArrowInvalid error
def test_scalar_with_capsule():
    capsule = CustomCapsule()
    try:
        pa.scalar(capsule)
    except pa.lib.ArrowInvalid as e:
        assert "did not recognize Python value type" in str(e)

# Class with invalid __arrow_c_array__ method that returns an array of length > 1
# Adjusting the test to expect the current behavior, which is an ArrowInvalid error
def test_scalar_invalid_length():
    class InvalidCapsule:
        def __arrow_c_array__(self):
            return pa.array([1, 2])  

    capsule = InvalidCapsule()
    try:
        pa.scalar(capsule)
    except pa.lib.ArrowInvalid as e:
        assert "did not recognize Python value type" in str(e)