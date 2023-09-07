// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.C;
using System;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Acero
{
    public class CLib
    {
        public struct GError { }
        public struct GArrowExecutePlan { }
        public struct GArrowSourceNodeOptions { }
        public struct GArrowRecordBatch { }
        public struct GArrowSchema { }
        public struct GArrowField { }
        public struct GArrowExecuteNode { }
        public struct GArrowHashJoinNodeOptions { }
        public struct GArrowSinkNodeOptions { }
        public struct GArrowRecordBatchReader { }
        public struct GArrowExecuteNodeOptions { }
        public struct GArrowFilterNodeOptions { }
        public struct GArrowExpression { }
        public struct GArrowCallExpression { }
        public struct GArrowFunctionOptions { }
        public struct GArrowFieldExpression { }
        public struct GArrowDatum { }
        public struct GArrowScalarDatum { }
        public struct GArrowBuffer { }
        public struct GArrowStringScalar { }
        public struct GArrowInt8Scalar { }
        public struct GArrowSortOptions { }
        public struct GArrowSortKey { }
        public struct GArrowProjectNodeOptions { }
        public struct GArrowInt32Scalar { }

        public enum GArrowJoinType
        {
            GARROW_JOIN_TYPE_LEFT_SEMI,
            GARROW_JOIN_TYPE_RIGHT_SEMI,
            GARROW_JOIN_TYPE_LEFT_ANTI,
            GARROW_JOIN_TYPE_RIGHT_ANTI,
            GARROW_JOIN_TYPE_INNER,
            GARROW_JOIN_TYPE_LEFT_OUTER,
            GARROW_JOIN_TYPE_RIGHT_OUTER,
            GARROW_JOIN_TYPE_FULL_OUTER
        }

        public enum GArrowSortOrder
        {
            GARROW_SORT_ORDER_ASCENDING,
            GARROW_SORT_ORDER_DESCENDING
        }

        public const string DllName = "libarrow-glib-1300.dll";

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_new")]
        public static extern unsafe GArrowExecutePlan* garrow_execute_plan_new(out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_source_node_options_new_record_batch")]
        public static extern unsafe GArrowSourceNodeOptions* garrow_source_node_options_new_record_batch(GArrowRecordBatch* record_batch);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_record_batch_import")]
        public static extern unsafe GArrowRecordBatch* garrow_record_batch_import(CArrowArray* c_abi_array, GArrowSchema* schema, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_schema_import")]
        public static extern unsafe GArrowSchema* garrow_schema_import(CArrowSchema* c_abi_schema, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_schema_get_field")]
        public static extern unsafe GArrowField* garrow_schema_get_field(GArrowSchema* schema, uint i);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_field_is_nullable")]
        public static extern unsafe bool garrow_field_is_nullable(GArrowField* field);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_hash_join_node_options_new")]
        public static extern unsafe GArrowHashJoinNodeOptions* garrow_hash_join_node_options_new(GArrowJoinType type, IntPtr left_keys, uint n_left_keys, IntPtr right_keys, uint n_right_keys, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_hash_join_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_hash_join_node(GArrowExecutePlan* plan, GArrowExecuteNode* left, GArrowExecuteNode* right, GArrowHashJoinNodeOptions* options, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_source_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_source_node(GArrowExecutePlan* plan, GArrowSourceNodeOptions* options, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_validate")]
        public static extern unsafe bool garrow_execute_plan_validate(GArrowExecutePlan* plan, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_sink_node_options_new")]
        public static extern unsafe GArrowSinkNodeOptions* garrow_sink_node_options_new();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_sink_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_sink_node(GArrowExecutePlan* plan, GArrowExecuteNode* input, GArrowSinkNodeOptions* options, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_start")]
        public static extern unsafe void garrow_execute_plan_start(GArrowExecutePlan* plan);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_wait")]
        public static extern unsafe void garrow_execute_plan_wait(GArrowExecutePlan* plan);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_sink_node_options_get_reader")]
        public static extern unsafe GArrowRecordBatchReader* garrow_sink_node_options_get_reader(GArrowSinkNodeOptions* options, GArrowSchema* schema);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_record_batch_reader_export")]
        public static extern unsafe CArrowArrayStream* garrow_record_batch_reader_export(GArrowRecordBatchReader* reader, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_record_batch_reader_import")]
        public static extern unsafe GArrowRecordBatchReader* garrow_record_batch_reader_import(CArrowArrayStream* c_abi_array_stream, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_source_node_options_new_record_batch_reader")]
        public static extern unsafe GArrowSourceNodeOptions* garrow_source_node_options_new_record_batch_reader(GArrowRecordBatchReader* reader);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_filter_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_filter_node(GArrowExecutePlan* plan, GArrowExecuteNode* input, GArrowFilterNodeOptions* options, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_filter_node_options_new")]
        public static extern unsafe GArrowFilterNodeOptions* garrow_filter_node_options_new(IntPtr expression);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_call_expression_new")]
        public static extern unsafe GArrowCallExpression* garrow_call_expression_new(IntPtr function, IntPtr arguments, GArrowFunctionOptions* options);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_field_expression_new")]
        public static extern unsafe GArrowFieldExpression* garrow_field_expression_new(IntPtr reference, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_literal_expression_new")]
        public static extern unsafe GArrowFieldExpression* garrow_literal_expression_new(GArrowDatum* datum);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_scalar_datum_new")]
        public static extern unsafe GArrowScalarDatum* garrow_scalar_datum_new(IntPtr value);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_buffer_new")]
        public static extern unsafe GArrowBuffer* garrow_buffer_new(IntPtr data, long size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_buffer_new_bytes")]
        public static extern unsafe GArrowBuffer* garrow_buffer_new_bytes(IntPtr data);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_string_scalar_new")]
        public static extern unsafe GArrowStringScalar* garrow_string_scalar_new(GArrowBuffer* value);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_expression_to_string")]
        public static extern unsafe IntPtr garrow_expression_to_string(IntPtr expression);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_int64_scalar_new")]
        public static extern unsafe GArrowInt8Scalar* garrow_int64_scalar_new(long value);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_sort_options_new")]
        public static extern unsafe GArrowSortOptions* garrow_sort_options_new(GList* sort_keys);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_sort_key_new")]
        public static extern unsafe GArrowSortKey* garrow_sort_key_new(IntPtr target, GArrowSortOrder order, out GError** error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_node(GArrowExecutePlan* plan, IntPtr factory_name, IntPtr inputs, IntPtr options, out GError **error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_project_node_options_new")]
        public static extern unsafe GArrowProjectNodeOptions* garrow_project_node_options_new(GList* expressions, IntPtr names, int n_names);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_execute_plan_build_project_node")]
        public static extern unsafe GArrowExecuteNode* garrow_execute_plan_build_project_node(GArrowExecutePlan* plan, GArrowExecuteNode* input, GArrowProjectNodeOptions* options, out GError** error);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "garrow_int32_scalar_new")]
        public static extern unsafe GArrowInt32Scalar* garrow_int32_scalar_new(int value);
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct GList
    {
        public IntPtr data;
        public GList* next;
        public GList* prev;
    }
}
