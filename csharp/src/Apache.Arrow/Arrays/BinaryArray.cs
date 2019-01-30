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
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    public class BinaryArray: Array
    {
        public BinaryArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Binary);
            data.EnsureBufferCount(3);
        }

        public BinaryArray(ArrowTypeId typeId, ArrayData data)
            : base (data)
        {
            data.EnsureDataType(typeId);
            data.EnsureBufferCount(3);
        }

        public BinaryArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
        : this(new ArrayData(dataType, length, nullCount, offset, 
            new [] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ArrowBuffer ValueBuffer => Data.Buffers[2];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(0, Length + 1);

        public ReadOnlySpan<byte> Values => ValueBuffer.Span.CastTo<byte>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueOffset(int index)
        {
            return ValueOffsets[Offset + index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueLength(int index)
        {
            var offsets = ValueOffsets;
            var offset = Offset + index;

            return offsets[offset + 1] - offsets[offset];
        }

        public ReadOnlySpan<byte> GetBytes(int index)
        {
            var offset = GetValueOffset(index);
            var length = GetValueLength(index);
            
            return ValueBuffer.Span.Slice(offset, length);
        }

    }
}
