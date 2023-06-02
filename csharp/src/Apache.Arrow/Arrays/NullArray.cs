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
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public class NullArray : IArrowArray
    {
        public class Builder : IArrowArrayBuilder<NullArray, Builder>
        {
            private int _length;

            public int Length => _length;
            public int Capacity => _length;
            public int NullCount => _length;

            public Builder()
            {
            }

            public Builder AppendNull()
            {
                _length++;
                return this;
            }

            public NullArray Build(MemoryAllocator allocator = default)
            {
                return new NullArray(_length);
            }

            public Builder Clear()
            {
                _length = 0;
                return this;
            }

            public Builder Reserve(int capacity)
            {
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity));
                }

                return this;
            }

            public Builder Resize(int length)
            {
                if (length < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(length));
                }

                _length = length;
                return this;
            }
        }

        public ArrayData Data { get; }

        public NullArray(ArrayData data)
        {
            if (data.Length != data.NullCount)
            {
                throw new ArgumentException("Length must equal null count", nameof(data));
            }

            data.EnsureDataType(ArrowTypeId.Null);
            data.EnsureBufferCount(0);
            Data = data;
        }

        public NullArray(int length)
            : this(new ArrayData(NullType.Default, length, length, buffers: System.Array.Empty<ArrowBuffer>()))
        {
        }

        public int Length => Data.Length;

        public int Offset => Data.Offset;

        public int NullCount => Data.NullCount;

        public void Dispose() { }
        public bool IsNull(int index) => true;
        public bool IsValid(int index) => false;

        public void Accept(IArrowArrayVisitor visitor) => Array.Accept(this, visitor);
    }
}
