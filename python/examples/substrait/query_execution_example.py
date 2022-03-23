import pyarrow as pa
import pyarrow.compute as pc

query = """
({
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
  })
"""