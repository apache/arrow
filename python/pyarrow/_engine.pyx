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

# cython: language_level = 3

import sys

from cython.operator cimport dereference as deref

from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *
import pyarrow.lib as lib

import numpy as np

def run_query(plan, output_schema):

    cdef:
        CResult[shared_ptr[CRecordBatchReader]] c_res_reader
        shared_ptr[CRecordBatchReader] c_reader
        shared_ptr[CSchema] c_schema
        c_string c_plan
        RecordBatchReader reader

    c_plan = plan.encode()

    c_schema = pyarrow_unwrap_schema(output_schema)
    
    c_res_reader = GetRecordBatchReader(c_plan, c_schema)
    c_reader = GetResultValue(c_res_reader)

    reader = RecordBatchReader.__new__(RecordBatchReader)
    reader.reader = c_reader
    return reader