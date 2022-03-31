from typing import List
import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.compute import register_function
from pyarrow.compute import InputType


def get_function_doc(summary: str, desc: str, arg_names: List[str],
                     options_class: str, options_required: bool = False):
    func_doc = {}
    func_doc["summary"] = summary
    func_doc["description"] = desc
    func_doc["arg_names"] = arg_names
    func_doc["options_class"] = options_class
    func_doc["options_required"] = False
    return func_doc


"""
Array Usage
"""

# Example 1: Array Unary


def array_unary_example():
    print("=" * 80)
    print("Example 1: Array Unary")
    print("=" * 80)

    def add_one(array):
        return pc.call_function("add", [array, 1])

    func_name = "py_add_one_func"
    arity = 1
    in_types = [InputType.array(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc("add function", "add function",
                           ["value"], "None")
    register_function(func_name, arity, doc,
                      in_types, out_type, add_one)

    func = pc.get_function(func_name)
    
    print(func)

    assert func.name == func_name

    res = pc.call_function(func_name, [pa.array([20])])

    print(res)

    res = pc.call_function(func_name, [pa.array([30])])

    print(res)


# Example 2: Array Binary

def array_binary_example():
    print("=" * 80)
    print("Example 2: Array Binary")
    print("=" * 80)
    arity = 2
    func_name = "array_udf_binary_add"
    in_types = [InputType.array(pa.int64()), InputType.array(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc(
        "array bin add function",
        "array bin add function",
        ["array_value1", "array_value2"], "None")

    def binary_array_function(array1, array2):
        return pc.call_function("add", [array1, array2])

    register_function(func_name, arity, doc,
                      in_types, out_type, binary_array_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.array([10, 11]), pa.array([20, 21])])

    print(ans)

    ans = pc.call_function(func_name, [pa.array([1, 2]), pa.array([10, 20])])

    print(ans)


# Example 3: Array Ternary

def array_ternary_example():
    print("=" * 80)
    print("Example 3: Array Ternary")
    print("=" * 80)
    arity = 3
    func_name = "array_udf_ternary_add"
    in_types = [InputType.array(pa.int64()),
                InputType.array(pa.int64()),
                InputType.array(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc(
        "array ternary add function",
        "array ternary add function",
        ["array_value1", "array_value2", "array_value3"], "None")

    def ternary_array_function(array1, array2, array3):
        return pc.call_function("add",
                                [pc.call_function("add", [array1, array2]),
                                 array3])

    register_function(func_name, arity, doc,
                      in_types, out_type, ternary_array_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.array([10, 11]),
                                       pa.array([20, 21]),
                                       pa.array([30, 31])])

    print(ans)

    ans = pc.call_function(func_name, [pa.array([1, 2]),
                                       pa.array([10, 20]),
                                       pa.array([100, 200])
                                       ])

    print(ans)


# Example 4: Array VarArgs
def array_varargs_example():
    print("=" * 80)
    print("Example 4: Array VarArgs")
    print("=" * 80)
    arity = 4
    func_name = "array_udf_varargs_add"
    in_types = [InputType.array(pa.int64()),
                InputType.array(pa.int64()),
                InputType.array(pa.int64()),
                InputType.array(pa.int64())
                ]
    out_type = pa.int64()
    doc = get_function_doc(
        "array varargs add function",
        "array varargs add function",
        ["array_value1", "array_value2",
         "array_value3", "array_value4"],
        "None")

    def varargs_array_function(array1, array2, array3, array4):
        array12 = pc.call_function("add", [array1, array2])
        array34 = pc.call_function("add", [array3, array4])
        return pc.call_function("add", [array12, array34])

    register_function(func_name, arity, doc,
                      in_types, out_type, varargs_array_function)

    func = pc.get_function(func_name)
    
    print(func)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.array([10, 11]),
                                       pa.array([20, 21]),
                                       pa.array([30, 31]),
                                       pa.array([40, 41])])

    print(ans)

    ans = pc.call_function(func_name, [pa.array([1, 2]),
                                       pa.array([10, 20]),
                                       pa.array([100, 200]),
                                       pa.array([1000, 2000])
                                       ])

    print(ans)


"""
Scalar Usage
"""

# Example 5: Scalar Unary


def scalar_unary_example():
    print("=" * 80)
    print("Example 5: Scalar Unary ")
    print("=" * 80)

    def unary_scalar_function(scalar):
        return pc.call_function("add", [scalar, 1])

    arity = 1
    func_name = "py_scalar_add_func"
    in_types = [InputType.scalar(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc("scalar add function", "scalar add function",
                           ["scalar_value"], "None")
    register_function(func_name, arity, doc, in_types,
                      out_type, unary_scalar_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.scalar(10)])

    print(ans)

    ans = pc.call_function(func_name, [pa.scalar(1)])

    print(ans)


# Example 6: Scalar Binary
def scalar_binary_example():
    print("=" * 80)
    print("Example 6: Scalar Binary")
    print("=" * 80)
    arity = 2
    func_name = "scalar_udf_binary_add"
    in_types = [InputType.scalar(pa.int64()), InputType.scalar(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc(
        "scalar bin add function",
        "scalar bin add function",
        ["scalar_value1", "scalar_value2"], "None")

    def binary_scalar_function(scalar1, scalar2):
        return pc.call_function("add", [scalar1, scalar2])

    register_function(func_name, arity, doc,
                      in_types, out_type, binary_scalar_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.scalar(10), pa.scalar(20)])

    print(ans)

    ans = pc.call_function(func_name, [pa.scalar(50), pa.scalar(30)])

    print(ans)

# Example 8: Scalar Ternary


def scalar_ternary_function():
    print("=" * 80)
    print("Example 7: Scalar Ternary")
    print("=" * 80)
    arity = 3
    func_name = "scalar_udf_ternary_add"
    in_types = [InputType.scalar(pa.int64()),
                InputType.scalar(pa.int64()),
                InputType.scalar(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc(
        "scalar ternary add function",
        "scalar ternary add function",
        ["scalar_value1", "scalar_value2",
         "scalar_value3"], "None")

    def ternary_scalar_function(scalar1, scalar2, scalar3):
        return pc.call_function("add",
                                [pc.call_function("add",
                                                  [scalar1, scalar2]),
                                 scalar3])

    register_function(func_name, arity, doc,
                      in_types, out_type, ternary_scalar_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(
        func_name, [pa.scalar(10), pa.scalar(20), pa.scalar(30)])

    print(ans)

    ans = pc.call_function(
        func_name, [pa.scalar(1), pa.scalar(2), pa.scalar(3)])

    print(ans)


# Example 8: Scalar VarArgs
def scalar_varargs_function():
    print("=" * 80)
    print("Example 8: Scalar VarArgs")
    print("=" * 80)
    arity = 4
    func_name = "scalar_udf_varargs_add"
    in_types = [InputType.scalar(pa.int64()),
                InputType.scalar(pa.int64()),
                InputType.scalar(pa.int64()),
                InputType.scalar(pa.int64())]
    out_type = pa.int64()

    doc = get_function_doc(
        "scalar ternary add function",
        "scalar ternary add function",
        ["scalar_value1",
         "scalar_value2",
         "scalar_value3",
         "scalar_value4"], "None")

    def varargs_scalar_function(scalar1, scalar2, scalar3, scalar4):
        return pc.call_function("add",
                                [pc.call_function("add",
                                                  [pc.call_function("add",
                                                                    [scalar1,
                                                                     scalar2]),
                                                   scalar3]), scalar4])

    register_function(func_name, arity, doc,
                      in_types, out_type, varargs_scalar_function)

    func = pc.get_function(func_name)

    assert func.name == func_name

    ans = pc.call_function(func_name, [pa.scalar(
        10), pa.scalar(20), pa.scalar(30), pa.scalar(40)])

    print(ans)

    ans = pc.call_function(func_name, [pa.scalar(
        1), pa.scalar(2), pa.scalar(3), pa.scalar(4)])

    print(ans)


if __name__ == '__main__':

    # scalar function examples
    scalar_unary_example()
    scalar_binary_example()
    scalar_ternary_function()
    scalar_varargs_function()

    # array function examples
    array_unary_example()
    array_binary_example()
    array_ternary_example()
    array_varargs_example()
