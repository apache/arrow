from typing import List
import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.compute import register_function
from pyarrow.compute import Arity, InputType


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
print("=" * 80)
print("Example 1: Array Unary")
print("=" * 80)


def add_constant(array):
    return pc.call_function("add", [array, 1])


func_name_1 = "py_add_func"
arity_1 = Arity.unary()
in_types_1 = [InputType.array(pa.int64())]
out_type_1 = pa.int64()
doc_1 = get_function_doc("add function", "test add function",
                         ["value"], "None")
register_function(func_name_1, arity_1, doc_1,
                  in_types_1, out_type_1, add_constant)

func1 = pc.get_function(func_name_1)

a1_1 = pc.call_function(func_name_1, [pa.array([20])])

print(a1_1)

a1_2 = pc.call_function(func_name_1, [pa.array([30])])

print(a1_2)


# Example 2: Array Binary
print("=" * 80)
print("Example 2: Array Binary")
print("=" * 80)
arity_2 = Arity.binary()
func_name_2 = "array_udf_binary_add"
in_types_2 = [InputType.array(pa.int64()), InputType.array(pa.int64())]
out_type_2 = pa.int64()
array_binary_add_function_doc = get_function_doc(
    "array bin add function",
    "test array bin add function",
    ["array_value1", "array_value2"], "None")


def binary_array_function(array1, array2):
    return pc.call_function("add", [array1, array2])


register_function(func_name_2, arity_2, array_binary_add_function_doc,
                  in_types_2, out_type_2, binary_array_function)

func2 = pc.get_function(func_name_2)

a2_1 = pc.call_function(func_name_2, [pa.array([10, 11]), pa.array([20, 21])])

print(a2_1)

a2_2 = pc.call_function(func_name_2, [pa.array([1, 2]), pa.array([10, 20])])

print(a2_2)


# Example 3: Array Ternary
print("=" * 80)
print("Example 3: Array Ternary")
print("=" * 80)
arity_3 = Arity.ternary()
func_name_3 = "array_udf_ternary_add"
in_types_3 = [InputType.array(pa.int64()),
              InputType.array(pa.int64()),
              InputType.array(pa.int64())]
out_type_3 = pa.int64()
array_ternary_add_function_doc = get_function_doc(
    "array ternary add function",
    "test array ternary add function",
    ["array_value1", "array_value2", "array_value3"], "None")


def ternary_array_function(array1, array2, array3):
    return pc.call_function("add",
                            [pc.call_function("add", [array1, array2]),
                             array3])


register_function(func_name_3, arity_3, array_ternary_add_function_doc,
                  in_types_3, out_type_3, ternary_array_function)

func3 = pc.get_function(func_name_3)

a3_1 = pc.call_function(func_name_3, [pa.array([10, 11]),
                                      pa.array([20, 21]),
                                      pa.array([30, 31])])

print(a3_1)

a3_2 = pc.call_function(func_name_3, [pa.array([1, 2]),
                                      pa.array([10, 20]),
                                      pa.array([100, 200])
                                      ])

print(a3_2)


# Example 4: Array VarArgs
print("=" * 80)
print("Example 4: Array VarArgs")
print("=" * 80)
arity_4 = Arity.varargs(4)
func_name_4 = "array_udf_varargs_add"
in_types_4 = [InputType.array(pa.int64()),
              InputType.array(pa.int64()),
              InputType.array(pa.int64()),
              InputType.array(pa.int64())
              ]
out_type_4 = pa.int64()
array_varargs_add_function_doc = get_function_doc(
    "array varargs add function",
    "test array varargs add function",
    ["array_value1", "array_value2",
     "array_value3", "array_value4"],
    "None")


def varargs_array_function(array1, array2, array3, array4):
    array12 = pc.call_function("add", [array1, array2])
    array34 = pc.call_function("add", [array3, array4])
    return pc.call_function("add", [array12, array34])


register_function(func_name_4, arity_4, array_varargs_add_function_doc,
                  in_types_4, out_type_4, varargs_array_function)

func4 = pc.get_function(func_name_4)

a4_1 = pc.call_function(func_name_4, [pa.array([10, 11]),
                                      pa.array([20, 21]),
                                      pa.array([30, 31]),
                                      pa.array([40, 41])])

print(a4_1)

a4_2 = pc.call_function(func_name_4, [pa.array([1, 2]),
                                      pa.array([10, 20]),
                                      pa.array([100, 200]),
                                      pa.array([1000, 2000])
                                      ])

print(a4_2)


"""
Scalar Usage
"""

# Example 5: Scalar Unary

print("=" * 80)
print("Example 5: Scalar Unary ")
print("=" * 80)


def unary_scalar_function(scalar):
    return pc.call_function("add", [scalar, 1])


arity_5 = Arity.unary()
func_name_5 = "py_scalar_add_func"
in_types_5 = [InputType.scalar(pa.int64())]
out_type_5 = pa.int64()
doc_5 = get_function_doc("scalar add function", "test scalar add function",
                         ["scalar_value"], "None")
register_function(func_name_5, arity_5, doc_5, in_types_5,
                  out_type_5, unary_scalar_function)

func5 = pc.get_function(func_name_5)

a5_1 = pc.call_function(func_name_5, [pa.scalar(10)])

print(a5_1)

a5_2 = pc.call_function(func_name_5, [pa.scalar(1)])

print(a5_2)


# Example 6: Scalar Binary
print("=" * 80)
print("Example 6: Scalar Binary")
print("=" * 80)
arity_6 = Arity.binary()
func_name_6 = "scalar_udf_binary_add"
in_types_6 = [InputType.scalar(pa.int64()), InputType.scalar(pa.int64())]
out_type_6 = pa.int64()
scalar_binary_add_function_doc = get_function_doc(
    "scalar bin add function",
    "test scalar bin add function",
    ["scalar_value1", "scalar_value2"], "None")


def binary_scalar_function(scalar1, scalar2):
    return pc.call_function("add", [scalar1, scalar2])


register_function(func_name_6, arity_6, scalar_binary_add_function_doc,
                  in_types_6, out_type_6, binary_scalar_function)

func6 = pc.get_function(func_name_6)

a6_1 = pc.call_function(func_name_6, [pa.scalar(10), pa.scalar(20)])

print(a6_1)

a6_2 = pc.call_function(func_name_6, [pa.scalar(50), pa.scalar(30)])

print(a6_2)

# Example 8: Scalar Ternary
print("=" * 80)
print("Example 7: Scalar Ternary")
print("=" * 80)
arity_7 = Arity.ternary()
func_name_7 = "scalar_udf_ternary_add"
in_types_7 = [InputType.scalar(pa.int64()),
              InputType.scalar(pa.int64()),
              InputType.scalar(pa.int64())]
out_type_7 = pa.int64()
scalar_ternary_add_function_doc = get_function_doc(
    "scalar ternary add function",
    "test scalar ternary add function",
    ["scalar_value1", "scalar_value2",
     "scalar_value3"], "None")


def ternary_scalar_function(scalar1, scalar2, scalar3):
    return pc.call_function("add",
                            [pc.call_function("add",
                                              [scalar1, scalar2]),
                             scalar3])


register_function(func_name_7, arity_7, scalar_ternary_add_function_doc,
                  in_types_7, out_type_7, ternary_scalar_function)

func7 = pc.get_function(func_name_7)

a7_1 = pc.call_function(
    func_name_7, [pa.scalar(10), pa.scalar(20), pa.scalar(30)])

print(a7_1)

a7_2 = pc.call_function(
    func_name_7, [pa.scalar(1), pa.scalar(2), pa.scalar(3)])

print(a7_2)


# Example 8: Scalar VarArgs
print("=" * 80)
print("Example 8: Scalar VarArgs")
print("=" * 80)
arity_8 = Arity.varargs(4)
func_name_8 = "scalar_udf_varargs_add"
in_types_8 = [InputType.scalar(pa.int64()),
              InputType.scalar(pa.int64()),
              InputType.scalar(pa.int64()),
              InputType.scalar(pa.int64())]
out_type_8 = pa.int64()

scalar_ternary_add_function_doc = get_function_doc(
    "scalar ternary add function",
    "test scalar ternary add function",
    ["scalar_value1",
     "scalar_value2",
     "scalar_value3",
     "scalar_value4"], "None")


def ternary_scalar_function(scalar1, scalar2, scalar3, scalar4):
    return pc.call_function("add",
                            [pc.call_function("add",
                                              [pc.call_function("add",
                                                                [scalar1,
                                                                 scalar2]),
                                               scalar3]), scalar4])


register_function(func_name_8, arity_8, scalar_ternary_add_function_doc,
                  in_types_8, out_type_8, ternary_scalar_function)

func8 = pc.get_function(func_name_8)

a8_1 = pc.call_function(func_name_8, [pa.scalar(
    10), pa.scalar(20), pa.scalar(30), pa.scalar(40)])

print(a8_1)

a8_2 = pc.call_function(func_name_8, [pa.scalar(
    1), pa.scalar(2), pa.scalar(3), pa.scalar(4)])

print(a8_2)
