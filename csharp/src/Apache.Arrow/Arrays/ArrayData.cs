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

using Apache.Arrow.Types;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public sealed class ArrayData
    {
        public readonly IArrowType DataType;
        public readonly int Length;
        public readonly int NullCount;
        public readonly int Offset;
        public readonly ArrowBuffer[] Buffers;
        public readonly ArrayData[] Children;

        public ArrayData(
            IArrowType dataType,
            int length, int nullCount = 0, int offset = 0,
            IEnumerable<ArrowBuffer> buffers = null, IEnumerable<ArrayData> children = null)
        {
            DataType = dataType ?? NullType.Default;
            Length = length;
            NullCount = nullCount;
            Offset = offset;
            Buffers = buffers?.ToArray();
            Children = children?.ToArray();
        }

        public ArrayData(
            IArrowType dataType,
            int length, int nullCount = 0, int offset = 0,
            ArrowBuffer[] buffers = null, ArrayData[] children = null)
        {
            DataType = dataType ?? NullType.Default;
            Length = length;
            NullCount = nullCount;
            Offset = offset;
            Buffers = buffers;
            Children = children;
        }
    }
}
