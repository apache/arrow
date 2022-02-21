import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.compute import call_function, register_pyfunction
from pyarrow.compute import Arity, InputType
func_doc = {}
func_doc["summary"] = "summary"
func_doc["description"] = "desc"
func_doc["arg_names"] = ["number"]
func_doc["options_class"] = "SomeOptions"
func_doc["options_required"] = False
arity = Arity.unary()
func_name = "python_udf"
in_types = [InputType.array(pa.int64())]
out_type = pa.int64()

def py_function(arrow_array):
    p_new_array = call_function("add", [arrow_array, 1])
    return p_new_array

def simple_function(args):
    print("=" * 80)
    print("Hello From Python")
    print("=" * 80)
    print(args)
    return args

def add_constant(array):
    return pc.call_function("add", [array, 1])


# example 1
print("=" * 80)
print("Example 1")
print("=" * 80)
callback = simple_function
register_pyfunction(func_name, arity, func_doc, in_types, out_type, callback) 

func1 = pc.get_function(func_name)

a1 = pc.call_function(func_name, [pa.array([20])])

print(a1)

# example 2
print("=" * 80)
print("Example 2")
print("=" * 80)
callback = add_constant
func_name = "py_add_func"
register_pyfunction(func_name, arity, func_doc, in_types, out_type, callback) 

func2 = pc.get_function(func_name)

a2 = pc.call_function(func_name, [pa.array([20])])

print(a2)
