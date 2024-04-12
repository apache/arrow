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
using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public class DenseUnionArray : UnionArray
    {
        public ArrowBuffer ValueOffsetBuffer => Data.Buffers[1];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetBuffer.Span.CastTo<int>();

        public DenseUnionArray(
            IArrowType dataType,
            int length,
            IEnumerable<IArrowArray> children,
            ArrowBuffer typeIds,
            ArrowBuffer valuesOffsetBuffer,
            int nullCount = 0,
            int offset = 0)
            : base(new ArrayData(
                dataType, length, nullCount, offset, new[] { typeIds, valuesOffsetBuffer },
                children.Select(child => child.Data)))
        {
            _fields = children.ToArray();
            ValidateMode(UnionMode.Dense, Type.Mode);
        }

        public DenseUnionArray(ArrayData data) 
            : base(data)
        {
            ValidateMode(UnionMode.Dense, Type.Mode);
            data.EnsureBufferCount(2);
        }

        protected override bool FieldIsValid(IArrowArray fieldArray, int index)
        {
            return fieldArray.IsValid(ValueOffsets[index]);
        }

        internal new static int ComputeNullCount(ArrayData data)
        {
            var offset = data.Offset;
            var length = data.Length;
            var typeIds = data.Buffers[0].Span.Slice(offset, length);
            var valueOffsets = data.Buffers[1].Span.CastTo<int>().Slice(offset, length);
            var childArrays = new IArrowArray[data.Children.Length];
            for (var childIdx = 0; childIdx < data.Children.Length; ++childIdx)
            {
                childArrays[childIdx] = ArrowArrayFactory.BuildArray(data.Children[childIdx]);
            }

            var nullCount = 0;
            for (var i = 0; i < length; ++i)
            {
                var typeId = typeIds[i];
                var valueOffset = valueOffsets[i];
                nullCount += childArrays[typeId].IsNull(valueOffset) ? 1 : 0;
            }

            return nullCount;
        }
    }
}
