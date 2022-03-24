import pyarrow as pa
import pyarrow.compute as pc

from pyarrow.engine import run_query

query = """
{
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ 
                         {"i64": {}},
                         {"bool": {}}
                       ]
            },
            "names": [
                      "i",
                       "b"
                     ]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://FILENAME_PLACEHOLDER",
                "format": "FILE_FORMAT_PARQUET"
              }
            ]
          }
        }
      }}
    ]
  }
"""

query = query.replace("FILENAME_PLACEHOLDER", "/Users/vibhatha/sandbox/parquet/example.parquet")

schema = pa.schema({"i": pa.int64(), "b": pa.bool_()})


reader = run_query(query, schema)

print(reader.read_all())

