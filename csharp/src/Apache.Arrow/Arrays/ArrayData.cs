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

using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using FlatBuffers;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public sealed class ArrayData : IDisposable
    {
        private const int RecalculateNullCount = -1;

        public readonly IArrowType DataType;
        public readonly int Length;
        public readonly int NullCount;
        public readonly int Offset;
        public readonly ArrowBuffer[] Buffers;
        public readonly ArrayData[] Children;
        public readonly ArrayData Dictionary; // Only used for dictionary type

        // This is left for compatibility with lower version binaries
        // before the dictionary type was supported.
        public ArrayData(
            IArrowType dataType,
            int length, int nullCount, int offset,
            IEnumerable<ArrowBuffer> buffers, IEnumerable<ArrayData> children) :
            this(dataType, length, nullCount, offset, buffers, children, null)
        { }

        // This is left for compatibility with lower version binaries
        // before the dictionary type was supported.
        public ArrayData(
            IArrowType dataType,
            int length, int nullCount, int offset,
            ArrowBuffer[] buffers, ArrayData[] children) :
            this(dataType, length, nullCount, offset, buffers, children, null)
        { }

        public ArrayData(
            IArrowType dataType,
            int length, int nullCount = 0, int offset = 0,
            IEnumerable<ArrowBuffer> buffers = null, IEnumerable<ArrayData> children = null, ArrayData dictionary = null)
        {
            DataType = dataType ?? NullType.Default;
            Length = length;
            NullCount = nullCount;
            Offset = offset;
            Buffers = buffers?.ToArray();
            Children = children?.ToArray();
            Dictionary = dictionary;
        }

        public ArrayData(
            IArrowType dataType,
            int length, int nullCount = 0, int offset = 0,
            ArrowBuffer[] buffers = null, ArrayData[] children = null, ArrayData dictionary = null)
        {
            DataType = dataType ?? NullType.Default;
            Length = length;
            NullCount = nullCount;
            Offset = offset;
            Buffers = buffers;
            Children = children;
            Dictionary = dictionary;
        }

        public void Dispose()
        {
            if (Buffers != null)
            {
                foreach (ArrowBuffer buffer in Buffers)
                {
                    buffer.Dispose();
                }
            }

            if (Children != null)
            {
                foreach (ArrayData child in Children)
                {
                    child?.Dispose();
                }
            }

            Dictionary?.Dispose();
        }

        public ArrayData Slice(int offset, int length)
        {
            if (offset > Length)
            {
                throw new ArgumentException($"Offset {offset} cannot be greater than Length {Length} for Array.Slice");
            }

            length = Math.Min(Length - offset, length);
            offset += Offset;

            return new ArrayData(DataType, length, RecalculateNullCount, offset, Buffers, Children, Dictionary);
        }

        public ArrayData Clone(MemoryAllocator allocator = default)
        {
            return new ArrayData(
                DataType,
                Length,
                NullCount,
                Offset,
                Buffers?.Select(b => b.Clone(allocator))?.ToArray(),
                Children?.Select(b => b.Clone(allocator))?.ToArray(),
                Dictionary?.Clone(allocator));
        }
    }
}
