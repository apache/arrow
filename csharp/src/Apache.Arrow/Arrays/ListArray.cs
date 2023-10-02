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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class ListArray : Array
    {
        public class Builder : IArrowArrayBuilder<ListArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => ValueOffsetsBufferBuilder.Length;

            private ArrowBuffer.Builder<int> ValueOffsetsBufferBuilder { get; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount { get; protected set; }

            private IArrowType DataType { get; }

            public Builder(IArrowType valueDataType) : this(new ListType(valueDataType))
            {
            }

            public Builder(Field valueField) : this(new ListType(valueField))
            {
            }

            internal Builder(ListType dataType)
            {
                ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType);
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                DataType = dataType;
            }

            /// <summary>
            /// Start a new variable-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder
            /// </summary>
            /// <returns></returns>
            public Builder Append()
            {
                ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
                ValidityBufferBuilder.Append(true);

                return this;
            }

            public Builder AppendNull()
            {
                ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
                ValidityBufferBuilder.Append(false);
                NullCount++;

                return this;
            }

            public ListArray Build(MemoryAllocator allocator = default)
            {
                ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);

                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBufferBuilder.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new ListArray(DataType, Length - 1,
                    ValueOffsetsBufferBuilder.Build(allocator), ValueBuilder.Build(allocator),
                    validityBuffer, NullCount, 0);
            }

            public Builder Reserve(int capacity)
            {
                ValueOffsetsBufferBuilder.Reserve(capacity + 1);
                ValidityBufferBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length + 1);
                ValidityBufferBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                ValueOffsetsBufferBuilder.Clear();
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                return this;
            }

        }

        public IArrowArray Values { get; }

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(Offset, Length + 1);

        public ListArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer }, new[] { values.Data }),
                values)
        {
        }

        public ListArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private ListArray(ArrayData data, IArrowArray values) : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.List);
            Values = values;
        }

        // Constructor for child MapArray
        internal ListArray(ArrayData data, IArrowArray values, ArrowTypeId typeId) : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(typeId);
            Values = values;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);


        [Obsolete("This method has been deprecated. Please use ValueOffsets[index] instead.")]
        public int GetValueOffset(int index)
        {
            if (index < 0 || index > Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            return ValueOffsets[index];
        }

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

            ReadOnlySpan<int> offsets = ValueOffsets;
            return offsets[index + 1] - offsets[index];
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

            return array.Slice(ValueOffsets[index], GetValueLength(index));
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
