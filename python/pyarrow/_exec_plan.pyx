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

# ---------------------------------------------------------------------
# Implement Internal ExecPlan bindings

# cython: profile=False
# distutils: language = c++
# cython: language_level = 3

from cython.operator cimport dereference as deref

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (Table, pyarrow_unwrap_table, pyarrow_wrap_table)
from pyarrow.lib import tobytes

cdef (shared_ptr[CAsyncExecBatchGenerator], shared_ptr[CExecPlan], shared_ptr[CRecordBatchReader]) execplan(t):
    cdef:
        CExecContext c_exec_context = CExecContext(c_default_memory_pool())
        shared_ptr[CExecPlan] c_exec_plan = GetResultValue(CExecPlan.Make(&c_exec_context))
        vector[CDeclaration] c_decls
        vector[CExecNode*] c_final_node_vec
        CExecNode *c_final_node
        CExecNode *c_sink_node
        CTable* c_table
        shared_ptr[CSourceNodeOptions] c_sourceopts
        shared_ptr[CSinkNodeOptions] c_sinkopts
        shared_ptr[CAsyncExecBatchGenerator] c_asyncexecbatchgen
        shared_ptr[CRecordBatchReader] c_recordbatchreader

    if isinstance(t, Table):
        c_table = pyarrow_unwrap_table(t).get()
    else:
        raise ValueError("Unsupproted type")
    
    c_sourceopts = GetResultValue(CSourceNodeOptions.FromTable(deref(c_table)))
    c_decls.push_back(CDeclaration(tobytes("source"), deref(c_sourceopts)))
    c_decls[0].label = tobytes("source")
    
    # Add Here additional nodes
    CDeclaration.Sequence(c_decls).AddToPlan(&deref(c_exec_plan))

    c_final_node_vec = deref(c_exec_plan).sinks()
    if c_final_node_vec.size() == 0:
        c_final_node_vec = deref(c_exec_plan).sources()
    c_final_node = c_final_node_vec[0]

    res = CSinkNodeOptions.MakeWithAsyncGenerator()
    c_sinkopts = res.first
    c_asyncexecbatchgen = res.second
    c_sink_node = GetResultValue(
        MakeExecNode(tobytes("sink"), &deref(c_exec_plan), c_final_node_vec, deref(c_sinkopts))
    )

    c_recordbatchreader = MakeGeneratorReader(c_final_node.output_schema(), 
                                              deref(c_asyncexecbatchgen),
                                              c_exec_context.memory_pool())

    deref(c_exec_plan).Validate()
    deref(c_exec_plan).StartProducing()

    return (c_asyncexecbatchgen, c_exec_plan, c_recordbatchreader)

def test():
    cdef:
        shared_ptr[CTable] c_table
        shared_ptr[CExecPlan] c_exec_plan
        shared_ptr[CAsyncExecBatchGenerator] c_asyncexecbatchgen
        shared_ptr[CRecordBatchReader] c_recordbatchreader
        shared_ptr[CSchema] c_schema

    t = Table.from_pydict({
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"]
    })
    
    res = execplan(t)
    c_asyncexecbatchgen = res[0]
    c_exec_plan = res[1]
    c_recordbatchreader = res[2]

    c_table = GetResultValue(CTable.FromRecordBatchReader(c_recordbatchreader.get()))
    table = pyarrow_wrap_table(c_table)
    
    deref(c_exec_plan).StopProducing()

    print(table)