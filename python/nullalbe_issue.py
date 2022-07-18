import json
import pyarrow.json as pj
import pyarrow as pa

s = {"id": "value", "nested": {"value": 1}}

with open("issue.json", "w") as write_file:
    json.dump(s, write_file, indent=4)

schema = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("nested", pa.struct([pa.field("value", pa.int64(), nullable=False)]))
])

table = pj.read_json('issue.json', parse_options=pj.ParseOptions(explicit_schema=schema))

print(schema)
print(table.schema)