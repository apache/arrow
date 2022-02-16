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
    """
    Internal Function to create and ExecPlan and run it.

    Parameters
    ----------
    inputs : list of Table or Dataset
             The sources from which the ExecPlan should fetch data.
             In most cases this is only one, unless the first node of the
             plan is able to get data from multiple different sources.
    output_type : Table or Dataset
             In which format the output should be provided.
    plan : vector[CDeclaration]
             The nodes of the plan that should be applied to the sources
             to produce the output.
    """
    cdef:
        CExecContext c_exec_context = CExecContext(c_default_memory_pool(),
                                                   GetCpuThreadPool())
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
    for ipt in inputs:
        if isinstance(ipt, Table):
            c_in_table = pyarrow_unwrap_table(ipt).get()
            c_sourceopts = GetResultValue(
                CSourceNodeOptions.FromTable(deref(c_in_table), c_exec_context.executor()))
        else:
            raise TypeError("Unsupproted type")

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

    # Create the output node
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

    # Start execution of the ExecPlan
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


def tables_join(join_type, left_table, left_keys, right_table, right_keys):
    """
    Perform join of two tables.

    The result will be an output table with the result of the join operation

    Parameters
    ----------
    join_type : str
        One of supported join types.
    left_table : Table
        The left table for the join operation
    left_keys : str or list[str]
        The left table key (or keys) on which the join operation should be performed
    right_table : Table
        The right table for the join operation
    right_keys : str or list[str]
        The right table key (or keys) on which the join operation should be performed

    Returns
    -------
    result_table : Table
    """
    cdef:
        vector[CFieldRef] c_left_keys
        vector[CFieldRef] c_right_keys
        vector[CDeclaration] c_decl_plan
        CJoinType c_join_type

    # Prepare left and right tables Keys to send them to the C++ function
    if isinstance(left_keys, str):
        left_keys = [left_keys]
    for key in left_keys:
        c_left_keys.push_back(CFieldRef(<c_string>tobytes(key)))

    if isinstance(right_keys, str):
        right_keys = [right_keys]
    for key in right_keys:
        c_right_keys.push_back(CFieldRef(<c_string>tobytes(key)))

    # Pick the join type
    if join_type == "left semi":
        c_join_type = CJoinType_LEFT_SEMI
    elif join_type == "right semi":
        c_join_type = CJoinType_RIGHT_SEMI
    elif join_type == "left anti":
        c_join_type = CJoinType_LEFT_ANTI
    elif join_type == "right anti":
        c_join_type = CJoinType_RIGHT_ANTI
    elif join_type == "inner":
        c_join_type = CJoinType_INNER
    elif join_type == "left outer":
        c_join_type = CJoinType_LEFT_OUTER
    elif join_type == "right outer":
        c_join_type = CJoinType_RIGHT_OUTER
    elif join_type == "full outer":
        c_join_type = CJoinType_FULL_OUTER
    else:
        raise ValueError("Unsupporter join type")

    # Add the join node to the execplan
    c_decl_plan.push_back(
        CDeclaration(tobytes("hashjoin"), CHashJoinNodeOptions(
            c_join_type, c_left_keys, c_right_keys
        ))
    )

    result_table = execplan([left_table, right_table],
                            output_type=Table,
                            plan=c_decl_plan)

    return result_table
