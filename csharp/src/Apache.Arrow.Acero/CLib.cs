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

using System;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Acero
{
    namespace CLib
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
        [StructLayout(LayoutKind.Sequential)]
        public struct GArrowExecuteNodeOptions
        {
            IntPtr debug_opts;
        }
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
        public struct GList {}

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
    }
}
