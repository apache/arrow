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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class LargeListArray : Array
    {
        public IArrowArray Values { get; }

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ReadOnlySpan<long> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<long>().Slice(Offset, Length + 1);

        public LargeListArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer }, new[] { values.Data }),
                values)
        {
        }

        public LargeListArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private LargeListArray(ArrayData data, IArrowArray values) : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.LargeList);
            Values = values;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int GetValueLength(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                return 0;
            }

            ReadOnlySpan<long> offsets = ValueOffsets;
            return checked((int)(offsets[index + 1] - offsets[index]));
        }

        public IArrowArray GetSlicedValues(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                return null;
            }

            if (!(Values is Array array))
            {
                return default;
            }

            return array.Slice(checked((int)ValueOffsets[index]), GetValueLength(index));
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Values?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
