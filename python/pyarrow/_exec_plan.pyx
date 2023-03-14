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
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib cimport Table, check_status, pyarrow_unwrap_table, pyarrow_wrap_table
from pyarrow.lib import tobytes
from pyarrow._compute cimport Expression, _SortOptions
from pyarrow._dataset cimport Dataset, Scanner
from pyarrow._dataset import InMemoryDataset, ScanNodeOptions

from pyarrow._acero import Declaration, TableSourceNodeOptions, FilterNodeOptions, HashJoinNodeOptions, ProjectNodeOptions

Initialize()  # Initialise support for Datasets in ExecPlan


def _dataset_to_decl(Dataset dataset, use_threads=True):
    decl = Declaration("scan", ScanNodeOptions(dataset, use_threads=use_threads))

    filter_expr = dataset._scan_options.get("filter")
    if filter_expr is not None:
        # Filters applied in CScanNodeOptions are "best effort" for the scan node itself,
        # so we always need to inject an additional Filter node to apply them for real.
        decl = Declaration.from_sequence(
            [decl, Declaration("filter", FilterNodeOptions(filter_expr))]
        )

    return decl


cdef execplan(inputs, output_type, vector[CDeclaration] plan, c_bool use_threads=True,
              _SortOptions sort_options=None):
    """
    Internal Function to create an ExecPlan and run it.

    Parameters
    ----------
    inputs : list of Table or Dataset
        The sources from which the ExecPlan should fetch data.
        In most cases this is only one, unless the first node of the
        plan is able to get data from multiple different sources.
    output_type : Table or InMemoryDataset
        In which format the output should be provided.
    plan : vector[CDeclaration]
        The nodes of the plan that should be applied to the sources
        to produce the output.
    use_threads : bool, default True
        Whether to use multithreading or not.
    """
    cdef:
        CExecutor *c_executor
        shared_ptr[CExecContext] c_exec_context
        shared_ptr[CExecPlan] c_exec_plan
        CDeclaration current_decl
        vector[CDeclaration] c_decls
        vector[CExecNode*] _empty
        vector[CExecNode*] c_final_node_vec
        CExecNode *c_node
        shared_ptr[CTable] c_in_table
        shared_ptr[CTable] c_out_table
        shared_ptr[CTableSourceNodeOptions] c_tablesourceopts
        shared_ptr[CScanNodeOptions] c_scanopts
        shared_ptr[CExecNodeOptions] c_input_node_opts
        shared_ptr[CSinkNodeOptions] c_sinkopts
        shared_ptr[COrderBySinkNodeOptions] c_orderbysinkopts
        shared_ptr[CAsyncExecBatchGenerator] c_async_exec_batch_gen
        shared_ptr[CRecordBatchReader] c_recordbatchreader
        vector[CDeclaration].iterator plan_iter
        vector[CDeclaration.Input] no_c_inputs
        CStatus c_plan_status

    if use_threads:
        c_executor = GetCpuThreadPool()
    else:
        c_executor = NULL

    # TODO(weston): This is deprecated.  Once ordering is better supported
    # in the exec plan we can remove all references to ExecPlan and use the
    # DeclarationToXyz methods
    c_exec_context = make_shared[CExecContext](
        c_default_memory_pool(), c_executor)
    c_exec_plan = GetResultValue(CExecPlan.Make(c_exec_context.get()))

    plan_iter = plan.begin()

    # Create source nodes for each input
    for ipt in inputs:
        if isinstance(ipt, Table):
            c_in_table = pyarrow_unwrap_table(ipt)
            c_tablesourceopts = make_shared[CTableSourceNodeOptions](
                c_in_table)
            c_input_node_opts = static_pointer_cast[CExecNodeOptions, CTableSourceNodeOptions](
                c_tablesourceopts)

            current_decl = CDeclaration(
                tobytes("table_source"), no_c_inputs, c_input_node_opts)
        elif isinstance(ipt, Dataset):
            c_in_dataset = (<Dataset>ipt).unwrap()
            c_scanopts = make_shared[CScanNodeOptions](
                c_in_dataset, Scanner._make_scan_options(ipt, {"use_threads": use_threads}))
            c_input_node_opts = static_pointer_cast[CExecNodeOptions, CScanNodeOptions](
                c_scanopts)

            # Filters applied in CScanNodeOptions are "best effort" for the scan node itself,
            # so we always need to inject an additional Filter node to apply them for real.
            current_decl = CDeclaration(
                tobytes("filter"),
                no_c_inputs,
                static_pointer_cast[CExecNodeOptions, CFilterNodeOptions](
                    make_shared[CFilterNodeOptions](
                        deref(deref(c_scanopts).scan_options).filter
                    )
                )
            )
            current_decl.inputs.push_back(
                CDeclaration.Input(
                    CDeclaration(tobytes("scan"), no_c_inputs, c_input_node_opts))
            )
        else:
            raise TypeError("Unsupported type")

        if plan_iter != plan.end():
            # Flag the source as the input of the first plan node.
            deref(plan_iter).inputs.push_back(CDeclaration.Input(current_decl))
        else:
            # Empty plan, make the source the first plan node.
            c_decls.push_back(current_decl)

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
    c_async_exec_batch_gen = make_shared[CAsyncExecBatchGenerator]()

    if sort_options is None:
        c_sinkopts = make_shared[CSinkNodeOptions](
            c_async_exec_batch_gen.get())
        GetResultValue(
            MakeExecNode(tobytes("sink"), &deref(c_exec_plan),
                         c_final_node_vec, deref(c_sinkopts))
        )
    else:
        c_orderbysinkopts = make_shared[COrderBySinkNodeOptions](
            deref(<CSortOptions*>(sort_options.unwrap().get())),
            c_async_exec_batch_gen.get()
        )
        GetResultValue(
            MakeExecNode(tobytes("order_by_sink"), &deref(c_exec_plan),
                         c_final_node_vec, deref(c_orderbysinkopts))
        )

    # Convert the asyncgenerator to a sync batch reader
    c_recordbatchreader = MakeGeneratorReader(c_node.output_schema(),
                                              deref(c_async_exec_batch_gen),
                                              deref(c_exec_context).memory_pool())

    # Start execution of the ExecPlan
    deref(c_exec_plan).Validate()
    deref(c_exec_plan).StartProducing()

    # Convert output to the expected one.
    c_out_table = GetResultValue(
        CTable.FromRecordBatchReader(c_recordbatchreader.get()))
    if output_type == Table:
        output = pyarrow_wrap_table(c_out_table)
    elif output_type == InMemoryDataset:
        output = InMemoryDataset(pyarrow_wrap_table(c_out_table))
    else:
        raise TypeError("Unsupported output type")

    with nogil:
        c_plan_status = deref(c_exec_plan).finished().status()
    check_status(c_plan_status)

    return output


def _perform_join(join_type, left_operand not None, left_keys,
                  right_operand not None, right_keys,
                  left_suffix=None, right_suffix=None,
                  use_threads=True, coalesce_keys=False,
                  output_type=Table):
    """
    Perform join of two tables or datasets.

    The result will be an output table with the result of the join operation

    Parameters
    ----------
    join_type : str
        One of supported join types.
    left_operand : Table or Dataset
        The left operand for the join operation.
    left_keys : str or list[str]
        The left key (or keys) on which the join operation should be performed.
    right_operand : Table or Dataset
        The right operand for the join operation.
    right_keys : str or list[str]
        The right key (or keys) on which the join operation should be performed.
    left_suffix : str, default None
        Which suffix to add to left column names. This prevents confusion
        when the columns in left and right operands have colliding names.
    right_suffix : str, default None
        Which suffix to add to the right column names. This prevents confusion
        when the columns in left and right operands have colliding names.
    use_threads : bool, default True
        Whether to use multithreading or not.
    coalesce_keys : bool, default False
        If the duplicated keys should be omitted from one of the sides
        in the join result.
    output_type: Table or InMemoryDataset
        The output type for the exec plan result.

    Returns
    -------
    result_table : Table or InMemoryDataset
    """
    # Prepare left and right tables Keys to send them to the C++ function
    left_keys_order = {}
    if not isinstance(left_keys, (tuple, list)):
        left_keys = [left_keys]
    for idx, key in enumerate(left_keys):
        left_keys_order[key] = idx

    right_keys_order = {}
    if not isinstance(right_keys, (list, tuple)):
        right_keys = [right_keys]
    for idx, key in enumerate(right_keys):
        right_keys_order[key] = idx

    # By default expose all columns on both left and right table
    left_columns = left_operand.schema.names
    right_columns = right_operand.schema.names

    # Pick the join type
    if join_type == "left semi" or join_type == "left anti":
        right_columns = []
    elif join_type == "right semi" or join_type == "right anti":
        left_columns = []
    elif join_type == "inner" or join_type == "left outer":
        right_columns = [
            col for col in right_columns if col not in right_keys_order
        ]
    elif join_type == "right outer":
        left_columns = [
            col for col in left_columns if col not in left_keys_order
        ]

    # Turn the columns to vectors of FieldRefs
    # and set aside indices of keys.
    left_column_keys_indices = {}
    for idx, colname in enumerate(left_columns):
        if colname in left_keys:
            left_column_keys_indices[colname] = idx
    right_column_keys_indices = {}
    for idx, colname in enumerate(right_columns):
        if colname in right_keys:
            right_column_keys_indices[colname] = idx

    # Add the join node to the execplan
    if isinstance(left_operand, Dataset):
        left_source = _dataset_to_decl(left_operand, use_threads=use_threads)
    else:
        left_source = Declaration("table_source", TableSourceNodeOptions(left_operand))
    if isinstance(right_operand, Dataset):
        right_source = _dataset_to_decl(right_operand, use_threads=use_threads)
    else:
        right_source = Declaration("table_source", TableSourceNodeOptions(right_operand))

    join_opts = HashJoinNodeOptions(
        join_type, left_keys, right_keys, left_columns, right_columns,
        output_suffix_for_left=left_suffix or "",
        output_suffix_for_right=right_suffix or "",
    )
    decl = Declaration("hashjoin", options=join_opts, inputs=[left_source, right_source])

    if coalesce_keys and join_type == "full outer":
        # In case of full outer joins, the join operation will output all columns
        # so that we can coalesce the keys and exclude duplicates in a subsequent projection.
        left_columns_set = set(left_columns)
        right_columns_set = set(right_columns)
        # Where the right table columns start.
        right_operand_index = len(left_columns)
        projected_col_names = []
        projections = []
        for idx, col in enumerate(left_columns + right_columns):
            if idx < len(left_columns) and col in left_column_keys_indices:
                # Include keys only once and coalesce left+right table keys.
                projected_col_names.append(col)
                # Get the index of the right key that is being paired
                # with this left key. We do so by retrieving the name
                # of the right key that is in the same position in the provided keys
                # and then looking up the index for that name in the right table.
                right_key_index = right_column_keys_indices[right_keys[left_keys_order[col]]]
                projections.append(
                    Expression._call("coalesce", [
                        Expression._field(idx), Expression._field(
                            right_operand_index+right_key_index)
                    ])
                )
            elif idx >= right_operand_index and col in right_column_keys_indices:
                # Do not include right table keys. As they would lead to duplicated keys.
                continue
            else:
                # For all the other columns incude them as they are.
                # Just recompute the suffixes that the join produced as the projection
                # would lose them otherwise.
                if left_suffix and idx < right_operand_index and col in right_columns_set:
                    col += left_suffix
                if right_suffix and idx >= right_operand_index and col in left_columns_set:
                    col += right_suffix
                projected_col_names.append(col)
                projections.append(
                    Expression._field(idx)
                )
        projection = Declaration(
            "project", ProjectNodeOptions(projections, projected_col_names)
        )
        decl = Declaration.from_sequence([decl, projection])

    result_table = decl.to_table(use_threads=use_threads)

    if output_type == Table:
        return result_table
    elif output_type == InMemoryDataset:
        return InMemoryDataset(result_table)
    else:
        raise TypeError("Unsupported output type")


def _filter_table(table, expression):
    """Filter rows of a table based on the provided expression.

    The result will be an output table with only the rows matching
    the provided expression.

    Parameters
    ----------
    table : Table or Dataset
        Table or Dataset that should be filtered.
    expression : Expression
        The expression on which rows should be filtered.

    Returns
    -------
    Table
    """
    decl = Declaration.from_sequence([
        Declaration("table_source", options=TableSourceNodeOptions(table)),
        Declaration("filter", options=FilterNodeOptions(expression))
    ])
    # TODO use_threads is set to False because this doesn't yet support
    # preserving the order of the in-memory table's batches
    return decl.to_table(use_threads=False)


def _sort_source(table_or_dataset, sort_options, output_type=Table):
    cdef:
        vector[CDeclaration] c_empty_decl_plan

    r = execplan([table_or_dataset],
                 plan=c_empty_decl_plan,
                 output_type=Table,
                 use_threads=True,
                 sort_options=sort_options)

    if output_type == Table:
        return r
    elif output_type == InMemoryDataset:
        # Get rid of special dataset columns
        # "__fragment_index", "__batch_index", "__last_in_fragment", "__filename"
        return InMemoryDataset(r.select(table_or_dataset.schema.names))
    else:
        raise TypeError("Unsupported output type")
