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
    public class FixedSizeListArray : Array
    {
        public class Builder : IArrowArrayBuilder<FixedSizeListArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length { get; protected set; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount => ValidityBufferBuilder.UnsetBitCount;

            private FixedSizeListType DataType { get; }

            private int ExpectedValueLength => Length * DataType.ListSize;

            public Builder(IArrowType valueDataType, int listSize) : this(new FixedSizeListType(valueDataType, listSize))
            {
            }

            public Builder(Field valueField, int listSize) : this(new FixedSizeListType(valueField, listSize))
            {
            }

            internal Builder(FixedSizeListType dataType)
            {
                ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType);
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                DataType = dataType;
            }

            /// <summary>
            /// Start a new fixed-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder
            /// </summary>
            /// <returns></returns>
            public Builder Append()
            {
                ValidateChildLength();

                ValidityBufferBuilder.Append(true);
                Length++;

                return this;
            }

            public Builder AppendNull()
            {
                ValidateChildLength();

                ValidityBufferBuilder.Append(false);
                for (int i = 0; i < DataType.ListSize; i++)
                {
                    ValueBuilder.AppendNull();
                }
                Length++;

                return this;
            }

            public FixedSizeListArray Build(MemoryAllocator allocator = default)
            {
                ValidateChildLength();

                int nullCount = NullCount;
                ArrowBuffer validityBuffer = nullCount > 0
                                        ? ValidityBufferBuilder.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new FixedSizeListArray(DataType, Length,
                    ValueBuilder.Build(allocator),
                    validityBuffer, nullCount, 0);
            }

            public Builder Reserve(int capacity)
            {
                ValidityBufferBuilder.Reserve(capacity);
                ValueBuilder.Reserve(DataType.ListSize * capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValidateChildLength();

                ValidityBufferBuilder.Resize(length);
                ValueBuilder.Resize(DataType.ListSize * length);
                Length = length;
                return this;
            }

            public Builder Clear()
            {
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                Length = 0;
                return this;
            }

            void ValidateChildLength()
            {
                if (ValueBuilder.Length != ExpectedValueLength)
                {
                    int actualLength = ValueBuilder.Length - ExpectedValueLength + DataType.ListSize;
                    throw new ArgumentOutOfRangeException($"Lists of length: {actualLength} do not conform to the fixed size: " + DataType.ListSize);
                }
            }
        }

        public IArrowArray Values { get; }

        public FixedSizeListArray(IArrowType dataType, int length,
            IArrowArray values, ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer }, new[] { values.Data }),
                values)
        {
        }

        public FixedSizeListArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private FixedSizeListArray(ArrayData data, IArrowArray values) : base(data)
        {
            data.EnsureBufferCount(1);
            data.EnsureDataType(ArrowTypeId.FixedSizeList);
            Values = values;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

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

            index += Data.Offset;

            int length = ((FixedSizeListType)Data.DataType).ListSize;
            return array.Slice(index * length, length);
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
