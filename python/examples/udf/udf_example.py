from typing import List
import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.compute import call_function, register_function
from pyarrow.compute import Arity, InputType

def get_function_doc(summary:str, desc:str, arg_names:List[str],
                     options_class:str, options_required:bool=False):
    func_doc = {}
    func_doc["summary"] = summary
    func_doc["description"] = desc
    func_doc["arg_names"] = arg_names
    func_doc["options_class"] = options_class
    func_doc["options_required"] = False
    return func_doc

arity_1 = Arity.unary()
func_name_1 = "python_udf"
# TODO: evaluate this properly, the input type can be a record_batch, array or a table
# Caveat, a recordbatch or a table does not have type information. 
in_types_1 = [InputType.array(pa.int64())]
# TODO: evaluate this properly, whether the output type can support table, array or recordbatch
out_type_1 = pa.int64()

def py_function(arrow_array):
    p_new_array = call_function("add", [arrow_array, 1])
    return p_new_array

def simple_function(args):
    print("=" * 80)
    print(f"Hello From Python : {args}")
    print("=" * 80)
    return args

# # example 1
# print("=" * 80)
# print("Example 1")
# print("=" * 80)
# doc_1 = get_function_doc("simple function", "test simple function",
#                                        ["message"], "None")
# register_function(func_name_1, arity_1, doc_1, in_types_1, out_type_1, simple_function) 

# func1 = pc.get_function(func_name_1)

# a1_1 = pc.call_function(func_name_1, [pa.array([20])])

# print(a1_1)

# a1_2 = pc.call_function(func_name_1, [pa.array([30])])

# print(a1_2)

# # example 2
# print("=" * 80)
# print("Example 2")
# print("=" * 80)

# def add_constant(array):
#     return pc.call_function("add", [array, 1])

# func_name_2 = "py_add_func"
# arity_2 = Arity.unary()
# in_types_2 = [InputType.array(pa.int64())]
# out_type_2 = pa.int64()
# doc_2 = get_function_doc("add function", "test add function",
#                                        ["value"], "None")
# register_function(func_name_2, arity_2, doc_2, in_types_2, out_type_2, add_constant) 

# func2 = pc.get_function(func_name_2)

# a2 = pc.call_function(func_name_2, [pa.array([20])])

# print(a2)

# # example 3

print("=" * 80)
print("Example 3")
print("=" * 80)

def unary_scalar_function(scalar):
    return pc.call_function("add", [scalar, 1])

arity_3 = Arity.unary()
func_name_3 = "py_scalar_add_func"
in_types_3 = [InputType.scalar(pa.int64())]
out_type_3 = pa.int64()
doc_3 = get_function_doc("scalar add function", "test scalar add function",
                                       ["scalar_value"], "None")
register_function(func_name_3, arity_3, doc_3, in_types_3, out_type_3, unary_scalar_function) 

func3 = pc.get_function(func_name_3)

a3_1 = pc.call_function(func_name_3, [pa.scalar(10)])

print(a3_1)

a3_2 = pc.call_function(func_name_3, [pa.scalar(100)])

print(a3_2)

## Binary Function [Scalar]
# print("=" * 80)
# print("Scalar Binary Example 4")
# print("=" * 80)
# arity_4 = Arity.binary()
# func_name_4 = "scalar_udf_binary_add"
# # TODO: evaluate this properly, the input type can be a record_batch, array or a table
# # Caveat, a recordbatch or a table does not have type information. 
# in_types_4 = [InputType.scalar(pa.int64()), InputType.scalar(pa.int64())]
# # TODO: evaluate this properly, whether the output type can support table, array or recordbatch
# out_type_4 = pa.int64()
# scalar_binary_add_function_doc = get_function_doc("scalar bin add function", 
#                                                   "test scalar bin add function",
#                                        ["scalar_value1", "scalar_value2"], "None")

# def binary_scalar_function(scalar1, scalar2):
#     return pc.call_function("add", [scalar1, scalar2])

# register_function(func_name_4, arity_4, scalar_binary_add_function_doc, in_types_4, out_type_4, binary_scalar_function) 

# func4 = pc.get_function(func_name_4)

# a4_1 = pc.call_function(func_name_4, [pa.scalar(10), pa.scalar(20)])

# print(a4_1)

# a4_2 = pc.call_function(func_name_4, [pa.scalar(50), pa.scalar(30)])

# print(a4_2)


## Binary Function [Array]
# print("=" * 80)
# print("Array Data Binary Function Example 5")
# print("=" * 80)
# arity_5 = Arity.binary()
# func_name_5 = "array_udf_binary_add"
# in_types_5 = [InputType.array(pa.int64()), InputType.array(pa.int64())]
# out_type_5 = pa.int64()
# array_binary_add_function_doc = get_function_doc("array bin add function", 
#                                                   "test array bin add function",
#                                        ["array_value1", "array_value2"], "None")

# def binary_array_function(array1, array2):
#     return pc.call_function("add", [array1, array2])

# register_function(func_name_5, arity_5, array_binary_add_function_doc, in_types_5, out_type_5, binary_array_function) 

# func5 = pc.get_function(func_name_5)

# a5_1 = pc.call_function(func_name_5, [pa.array([10, 11]), pa.array([20, 21])])

# print(a5_1)

# a5_2 = pc.call_function(func_name_5, [pa.array([1, 2]), pa.array([10, 20])])

# print(a5_2)





