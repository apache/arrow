import pyarrow as pa
from pyarrow.compute import register_function, call_function, register_pyfunction
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
    print("\t \tHello From Python")
    print(args)
    return args

callback = simple_function
args = tuple([12345])
register_pyfunction(func_name, arity, func_doc, in_types, out_type, callback, args) 

from pyarrow import compute as pc

func1 = pc.get_function(func_name)

a = pc.call_function(func_name, [pa.array([20])])

print(a)