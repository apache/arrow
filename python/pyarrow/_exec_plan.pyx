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
from pyarrow.lib import tobytes, _pc


cdef execplan(inputs, output_type, vector[CDeclaration] plan, c_bool use_threads=True):
    """
    Internal Function to create an ExecPlan and run it.

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
    use_threads : bool, default True
        Whenever to use multithreading or not.
    """
    cdef:
        CExecutor *c_executor
        shared_ptr[CThreadPool] c_executor_sptr
        shared_ptr[CExecContext] c_exec_context
        shared_ptr[CExecPlan] c_exec_plan
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

    if use_threads:
        c_executor = GetCpuThreadPool()
    else:
        c_executor_sptr = GetResultValue(CThreadPool.Make(1))
        c_executor = c_executor_sptr.get()

    c_exec_context = make_shared[CExecContext](
        c_default_memory_pool(), c_executor)
    c_exec_plan = GetResultValue(CExecPlan.Make(c_exec_context.get()))

    plan_iter = plan.begin()

    # Create source nodes for each input
    for ipt in inputs:
        if isinstance(ipt, Table):
            c_in_table = pyarrow_unwrap_table(ipt).get()
            c_sourceopts = GetResultValue(
                CSourceNodeOptions.FromTable(deref(c_in_table), deref(c_exec_context).executor()))
        else:
            raise TypeError("Unsupported type")

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
                                              deref(c_exec_context).memory_pool())

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


# Default filter for the JOIN operation, means to pick every row.
cdef CExpression _true = CMakeScalarExpression(
    <shared_ptr[CScalar]> make_shared[CBooleanScalar](True)
)


def tables_join(join_type, left_table not None, left_keys,
                right_table not None, right_keys,
                left_suffix=None, right_suffix=None,
                use_threads=True, deduplicate=False):
    """
    Perform join of two tables.

    The result will be an output table with the result of the join operation

    Parameters
    ----------
    join_type : str
        One of supported join types.
    left_table : Table
        The left table for the join operation.
    left_keys : str or list[str]
        The left table key (or keys) on which the join operation should be performed.
    right_table : Table
        The right table for the join operation.
    right_keys : str or list[str]
        The right table key (or keys) on which the join operation should be performed.
    left_suffix : str, default None
        Which suffix to add to right column names. This prevents confusion
        when the columns in left and right tables have colliding names.
    right_suffix : str, default None
        Which suffic to add to the left column names. This prevents confusion
        when the columns in left and right tables have colliding names.
    use_threads : bool, default True
        Whenever to use multithreading or not.
    deduplicate : bool, default False
        If the duplicated keys should be omitted from one of the sides
        in the join result.

    Returns
    -------
    result_table : Table
    """
    cdef:
        vector[CFieldRef] c_left_keys
        vector[CFieldRef] c_right_keys
        vector[CFieldRef] c_left_columns
        vector[CFieldRef] c_right_columns
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

    # By default expose all columns on both left and right table
    left_columns = left_table.column_names
    right_columns = right_table.column_names

    # Pick the join type
    if join_type == "left semi":
        c_join_type = CJoinType_LEFT_SEMI
        right_columns = []
    elif join_type == "right semi":
        c_join_type = CJoinType_RIGHT_SEMI
        left_columns = []
    elif join_type == "left anti":
        c_join_type = CJoinType_LEFT_ANTI
        right_columns = []
    elif join_type == "right anti":
        c_join_type = CJoinType_RIGHT_ANTI
        left_columns = []
    elif join_type == "inner":
        c_join_type = CJoinType_INNER
        right_columns = set(right_columns) - set(right_keys)
    elif join_type == "left outer":
        c_join_type = CJoinType_LEFT_OUTER
        right_columns = set(right_columns) - set(right_keys)
    elif join_type == "right outer":
        c_join_type = CJoinType_RIGHT_OUTER
        left_columns = set(left_columns) - set(left_keys)
    elif join_type == "full outer":
        c_join_type = CJoinType_FULL_OUTER
    else:
        raise ValueError("Unsupported join type")

    # Turn the columns to vectors of FieldRefs
    for colname in left_columns:
        c_left_columns.push_back(CFieldRef(<c_string>tobytes(colname)))
    for colname in right_columns:
        c_right_columns.push_back(CFieldRef(<c_string>tobytes(colname)))

    # Add the join node to the execplan
    if deduplicate:
        c_decl_plan.push_back(
            CDeclaration(tobytes("hashjoin"), CHashJoinNodeOptions(
                c_join_type, c_left_keys, c_right_keys,
                c_left_columns, c_right_columns,
                _true,
                <c_string>tobytes(left_suffix or ""),
                <c_string>tobytes(right_suffix or "")
            ))
        )
    else:
        c_decl_plan.push_back(
            CDeclaration(tobytes("hashjoin"), CHashJoinNodeOptions(
                c_join_type, c_left_keys, c_right_keys,
                _true,
                <c_string>tobytes(left_suffix or ""),
                <c_string>tobytes(right_suffix or "")
            ))
        )

    result_table = execplan([left_table, right_table],
                            output_type=Table,
                            plan=c_decl_plan)

    if deduplicate and join_type == "full outer":
        suffixed_left_keys = left_keys
        if left_suffix:
            suffixed_left_keys = [
                lk+left_suffix for lk in left_keys if lk in right_keys]
        suffixed_right_keys = right_keys
        if right_suffix:
            suffixed_right_keys = [
                rk+right_suffix for rk in right_keys if rk in left_keys]
        for leftkey, rightkey in zip(reversed(suffixed_left_keys), reversed(suffixed_right_keys)):
            result_table = result_table.set_column(
                0, leftkey,
                _pc().coalesce(result_table[leftkey], result_table[rightkey])
            )
        result_table = result_table.drop(suffixed_right_keys)

    return result_table
