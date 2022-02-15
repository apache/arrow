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

from cython.operator cimport dereference as deref, preincrement as inc

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (Table, pyarrow_unwrap_table, pyarrow_wrap_table)
from pyarrow.lib import tobytes


cdef execplan(inputs, output_type, vector[CDeclaration] plan):
    cdef:
        CExecContext c_exec_context = CExecContext(c_default_memory_pool())
        shared_ptr[CExecPlan] c_exec_plan = GetResultValue(CExecPlan.Make(&c_exec_context))
        vector[CDeclaration] c_decls
        vector[CExecNode*] _empty
        vector[CExecNode*] c_final_node_vec
        CExecNode *c_node
        CTable* c_table
        shared_ptr[CTable] c_out_table
        shared_ptr[CSourceNodeOptions] c_sourceopts
        shared_ptr[CSinkNodeOptions] c_sinkopts
        shared_ptr[CAsyncExecBatchGenerator] c_asyncexecbatchgen
        shared_ptr[CRecordBatchReader] c_recordbatchreader
        vector[CDeclaration].iterator plan_iter

    plan_iter = plan.begin()

    # Create source nodes for each input
    for idx, ipt in enumerate(inputs):
        if isinstance(ipt, Table):
            c_in_table = pyarrow_unwrap_table(ipt).get()
            c_sourceopts = GetResultValue(
                CSourceNodeOptions.FromTable(deref(c_in_table)))
        else:
            raise ValueError("Unsupproted type")

        if plan_iter != plan.end():
            # Flag the source as the input of the first plan node.
            deref(plan_iter).inputs.push_back(CDeclaration.Input(
                CDeclaration(tobytes("source"), deref(c_sourceopts))
            ))
        else:
            # Empty plan, make the source the first plan node.
            c_decls.push_back(
                CDeclaration(tobytes("source"), deref(c_sourceopts))
            )

    # Add Here additional nodes
    while plan_iter != plan.end():
        c_decls.push_back(deref(plan_iter))
        inc(plan_iter)

    # Add all CDeclarations to the plan
    c_node = GetResultValue(
        CDeclaration.Sequence(c_decls).AddToPlan(&deref(c_exec_plan))
    )
    c_final_node_vec.push_back(c_node)

    # Create the output node
    c_asyncexecbatchgen = make_shared[CAsyncExecBatchGenerator]()
    c_sinkopts = make_shared[CSinkNodeOptions](c_asyncexecbatchgen.get())
    GetResultValue(
        MakeExecNode(tobytes("sink"), &deref(c_exec_plan),
                     c_final_node_vec, deref(c_sinkopts))
    )

    # Convert the asyncgenerator to a sync batch reader
    c_recordbatchreader = MakeGeneratorReader(c_node.output_schema(),
                                              deref(c_asyncexecbatchgen),
                                              c_exec_context.memory_pool())

    # Start execution of the ExecPlan
    deref(c_exec_plan).Validate()
    deref(c_exec_plan).StartProducing()

    # Convert output to the expected one.
    if output_type == Table:
        c_out_table = GetResultValue(
            CTable.FromRecordBatchReader(c_recordbatchreader.get()))
        output = pyarrow_wrap_table(c_out_table)
    else:
        raise TypeError("Unsupported output type")

    deref(c_exec_plan).StopProducing()

    return output


def test():
    # Currently run with
    #     python -c "__import__('pyarrow._exec_plan')._exec_plan.test()"
    cdef:
        vector[CExpression] c_project_expr
        vector[c_string] c_project_names
        vector[CDeclaration] c_decl_plan

    t = Table.from_pydict({
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"]
    })

    t2 = Table.from_pydict({
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"]
    })

    c_project_expr.push_back(CMakeFieldExpression(tobytes("col1")))
    c_decl_plan.push_back(
        CDeclaration(tobytes("project"), CProjectNodeOptions(c_project_expr))
    )
    table = execplan([t], output_type=Table, plan=c_decl_plan)

    print(table)
