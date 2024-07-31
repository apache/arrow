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
    public class ListViewArray : Array
    {
        public class Builder : IArrowArrayBuilder<ListViewArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => ValueOffsetsBufferBuilder.Length;

            private ArrowBuffer.Builder<int> ValueOffsetsBufferBuilder { get; }

            private ArrowBuffer.Builder<int> SizesBufferBuilder { get; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount { get; protected set; }

            private IArrowType DataType { get; }

            private int Start { get; set; }

            public Builder(IArrowType valueDataType) : this(new ListViewType(valueDataType))
            {
            }

            public Builder(Field valueField) : this(new ListViewType(valueField))
            {
            }

            internal Builder(ListViewType dataType)
            {
                ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType);
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
                SizesBufferBuilder = new ArrowBuffer.Builder<int>();
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                DataType = dataType;
                Start = -1;
            }

            /// <summary>
            /// Start a new variable-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder. TODO: Consider adding builder APIs to support construction
            /// of overlapping lists.
            /// </summary>
            public Builder Append()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(true);

                return this;
            }

            public Builder AppendNull()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(false);
                ValueOffsetsBufferBuilder.Append(Start);
                SizesBufferBuilder.Append(0);
                NullCount++;
                Start = -1;

                return this;
            }

            private void AppendPrevious()
            {
                if (Start >= 0)
                {
                    ValueOffsetsBufferBuilder.Append(Start);
                    SizesBufferBuilder.Append(ValueBuilder.Length - Start);
                }
                Start = ValueBuilder.Length;
            }

            public ListViewArray Build(MemoryAllocator allocator = default)
            {
                AppendPrevious();

                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBufferBuilder.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new ListViewArray(DataType, Length,
                    ValueOffsetsBufferBuilder.Build(allocator), SizesBufferBuilder.Build(allocator),
                    ValueBuilder.Build(allocator),
                    validityBuffer, NullCount, 0);
            }

            public Builder Reserve(int capacity)
            {
                ValueOffsetsBufferBuilder.Reserve(capacity);
                SizesBufferBuilder.Reserve(capacity);
                ValidityBufferBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length);
                SizesBufferBuilder.Resize(length);
                ValidityBufferBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                ValueOffsetsBufferBuilder.Clear();
                SizesBufferBuilder.Clear();
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                return this;
            }

        }

        public IArrowArray Values { get; }

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(Offset, Length);

        public ArrowBuffer SizesBuffer => Data.Buffers[2];

        public ReadOnlySpan<int> Sizes => SizesBuffer.Span.CastTo<int>().Slice(Offset, Length);

        public ListViewArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, ArrowBuffer sizesBuffer, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer, sizesBuffer }, new[] { values.Data }),
                values)
        {
        }

        public ListViewArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private ListViewArray(ArrayData data, IArrowArray values) : base(data)
        {
            data.EnsureBufferCount(3);
            data.EnsureDataType(ArrowTypeId.ListView);
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

            return Sizes[index];
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
