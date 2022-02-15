import pyarrow as pa
from pyarrow.compute import UDFInterpreter, UDFSynthesizer
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

def udf():
	print("Hello From Python")

callback = udf
UDFSynthesizer.register_function(func_name, arity, func_doc, in_types, out_type, callback) 

from pyarrow import compute as pc

func1 = pc.get_function(func_name)

pc.call_function(func_name, [pa.array([20])])