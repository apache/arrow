# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import pyarrow as pa

from pyarrow.engine import run_query

"""
Data Sample
-----------

    i      b
0  10  False
1  20   True
2  30  False

Run Example
-----------

python3 query_execution_example.py --filename <path-to-parquet-file>
"""

parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="display a square of a given number",
                    type=str)
args = parser.parse_args()

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

query = query.replace("FILENAME_PLACEHOLDER", args.filename)

schema = pa.schema({"i": pa.int64(), "b": pa.bool_()})


reader = run_query(query, schema)

print(reader.read_all())
