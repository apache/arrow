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
# Low-level Acero bindings

# cython: profile=False
# distutils: language = c++
# cython: language_level = 3

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib cimport (Table, check_status, pyarrow_unwrap_table, pyarrow_wrap_table,
                          RecordBatchReader)
from pyarrow.lib import frombytes, tobytes
from pyarrow._compute cimport Expression, FunctionOptions, _ensure_field_ref
from pyarrow.compute import field

# Initialize()  # Initialise support for Datasets in ExecPlan


cdef class ExecNodeOptions(_Weakrefable):
    __slots__ = ()  # avoid mistakingly creating attributes

    cdef const CExecNodeOptions* get_options(self) except NULL:
        return self.wrapped.get()

    cdef void init(self, const shared_ptr[CExecNodeOptions]& sp):
        self.wrapped = sp

    cdef inline shared_ptr[CExecNodeOptions] unwrap(self):
        return self.wrapped

    # def __repr__(self):
    #     type_name = self.__class__.__name__
    #     # Remove {} so we can use our own braces
    #     string_repr = frombytes(self.get_options().ToString())[1:-1]
    #     return f"{type_name}({string_repr})"

    # def __eq__(self, FunctionOptions other):
    #     return self.get_options().Equals(deref(other.get_options()))


cdef class _TableSourceNodeOptions(ExecNodeOptions):

    def _set_options(self, Table table):
        cdef:
            shared_ptr[CTable] c_table

        c_table = pyarrow_unwrap_table(table)
        self.wrapped.reset(
            new CTableSourceNodeOptions(c_table)
        )


class TableSourceNodeOptions(_TableSourceNodeOptions):
    """
    A Source node which accepts a table.

    Parameters
    ----------
    table : pyarrow.Table
        The table which acts as the data source.
    """

    def __init__(self, Table table):
        self._set_options(table)


cdef class _FilterNodeOptions(ExecNodeOptions):

    def _set_options(self, Expression filter_expression not None):
        self.wrapped.reset(
            new CFilterNodeOptions(<CExpression>filter_expression.unwrap())
        )


class FilterNodeOptions(_FilterNodeOptions):
    """
    Make a node which excludes some rows from batches passed through it.

    The "filter" operation provides an option to define data filtering
    criteria. It selects rows matching a given expression. Filters can
    be written using pyarrow.compute.Expression.

    Parameters
    ----------
    filter_expression : pyarrow.compute.Expression

    """

    def __init__(self, Expression filter_expression):
        self._set_options(filter_expression)


cdef class _ProjectNodeOptions(ExecNodeOptions):

    def _set_options(self, expressions, names=None):
        cdef:
            Expression expr
            vector[CExpression] c_expressions
            vector[c_string] c_names

        for expr in expressions:
            c_expressions.push_back(expr.unwrap())

        if names is not None:
            if len(names) != len(expressions):
                raise ValueError("dd")

            for name in names:
                c_names.push_back(<c_string>tobytes(name))

            self.wrapped.reset(
                new CProjectNodeOptions(c_expressions, c_names)
            )
        else:
            self.wrapped.reset(
                new CProjectNodeOptions(c_expressions)
            )


class ProjectNodeOptions(_ProjectNodeOptions):
    """
    Make a node which executes expressions on input batches,
    producing new batches.

    The "project" operation rearranges, deletes, transforms, and
    creates columns. Each output column is computed by evaluating
    an expression against the source record batch.

    Parameters
    ----------
    expressions : list of pyarrow.compute.Expression
        List of expressions to evaluate against the source batch.
    names : list of str
        List of names for each of the ouptut columns (same length as
        `expressions`). If `names` is not provided, the string
        representations of exprs will be used.
    """

    def __init__(self, expressions, names=None):
        self._set_options(expressions, names)


cdef class _AggregateNodeOptions(ExecNodeOptions):

    def _set_options(self, aggregates, keys=None):
        cdef:
            CAggregate c_aggr
            vector[CAggregate] c_aggregations
            vector[CFieldRef] c_keys

        for arg_names, func_name, opts, name in aggregates:
            c_aggr.function = tobytes(func_name)
            if opts is not None:
                c_aggr.options = (<FunctionOptions?>opts).wrapped
            else:
                c_aggr.options = <shared_ptr[CFunctionOptions]>nullptr
            for arg in arg_names:
                c_aggr.target.push_back(_ensure_field_ref(arg))
            c_aggr.name = tobytes(name)

            c_aggregations.push_back(move(c_aggr))

        if keys is None:
            keys = []
        for name in keys:
            c_keys.push_back(_ensure_field_ref(name))

        self.wrapped.reset(
            new CAggregateNodeOptions(c_aggregations, c_keys)
        )


class AggregateNodeOptions(_AggregateNodeOptions):
    """
    Make a node which aggregates input batches, optionally grouped by keys.

    Acero supports two types of aggregates: "scalar" aggregates,
    and "hash" aggregates. Scalar aggregates reduce an array or scalar
    input to a single scalar output (e.g. computing the mean of a column).
    Hash aggregates act like GROUP BY in SQL and first partition data
    based on one or more key columns, then reduce the data in each partition.
    The aggregate node supports both types of computation, and can compute
    any number of aggregations at once.

    Parameters
    ----------
    aggregates : list of tuples
        Aggregations which will be applied to the targetted fields.
        Specified as a list of tuples, where each tuple is one aggregation
        specification and consists of: aggregation column name followed
        by function name, aggregation function options object and the
        output field name.
        The column name can be a string, an empty list or a list of
        column names, for unary, nullary and n-ary aggregation functions
        respectively.
    keys : list, optional
        Keys by which aggregations will be grouped.

    """

    def __init__(self, aggregates, keys=None):
        self._set_options(aggregates, keys)


cdef class Declaration(_Weakrefable):
    """

    """
    cdef void init(self, const CDeclaration& c_decl):
        self.decl = c_decl

    @staticmethod
    cdef wrap(const CDeclaration& c_decl):
        cdef Declaration self = Declaration.__new__(Declaration)
        self.init(c_decl)
        return self

    cdef inline CDeclaration unwrap(self) nogil:
        return self.decl

    def __init__(self, factory_name, ExecNodeOptions options, inputs=None):
        cdef:
            c_string c_factory_name
            CDeclaration c_decl
            vector[CDeclaration.Input] c_inputs

        c_factory_name = tobytes(factory_name)

        if inputs is not None:
            for ipt in inputs:
                c_inputs.push_back(
                    CDeclaration.Input((<Declaration>ipt).unwrap())
                )

        c_decl = CDeclaration(c_factory_name, c_inputs, options.unwrap())
        self.init(c_decl)

    @staticmethod
    def from_sequence(decls):
        cdef:
            vector[CDeclaration] c_decls
            CDeclaration c_decl

        for decl in decls:
            c_decls.push_back((<Declaration> decl).unwrap())

        c_decl = CDeclaration.Sequence(c_decls)
        return Declaration.wrap(c_decl)

    def __str__(self):
        return frombytes(GetResultValue(DeclarationToString(self.decl)))

    def __repr__(self):
        return "<pyarrow.acero.Declaration>\n{0}".format(str(self))

    def to_table(self, use_threads=True):
        cdef:
            shared_ptr[CTable] c_table

        c_table = GetResultValue(DeclarationToTable(self.decl, use_threads))
        return pyarrow_wrap_table(c_table)
