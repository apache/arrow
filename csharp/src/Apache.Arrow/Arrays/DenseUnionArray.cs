﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
    }
}
