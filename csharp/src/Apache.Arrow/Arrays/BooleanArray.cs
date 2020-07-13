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
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    public class BooleanArray: Array
    {
        public class Builder : IArrowArrayBuilder<bool, BooleanArray, Builder>
        {
            private ArrowBuffer.BitmapBuilder ValueBuffer { get; }
            private ArrowBuffer.BitmapBuilder ValidityBuffer { get; }

            public int Length => ValueBuffer.Length;
            public int Capacity => ValueBuffer.Capacity;
            public int NullCount => ValidityBuffer.UnsetBitCount;

            public Builder()
            {
                ValueBuffer = new ArrowBuffer.BitmapBuilder();
                ValidityBuffer = new ArrowBuffer.BitmapBuilder();
            }

            public Builder Append(bool value)
            {
                return NullableAppend(value);
            }

            public Builder NullableAppend(bool? value)
            {
                // Note that we rely on the fact that null values are false in the value buffer.
                ValueBuffer.Append(value ?? false);
                ValidityBuffer.Append(value.HasValue);
                return this;
            }

            public Builder Append(ReadOnlySpan<bool> span)
            {
                foreach (bool value in span)
                {
                    Append(value);
                }
                return this;
            }

            public Builder AppendRange(IEnumerable<bool> values)
            {
                foreach (bool value in values)
                {
                    Append(value);
                }
                return this;
            }

            public Builder AppendNull()
            {
                return NullableAppend(null);
            }

            public BooleanArray Build(MemoryAllocator allocator = default)
            {
                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBuffer.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new BooleanArray(
                    ValueBuffer.Build(allocator), validityBuffer,
                    Length, NullCount, 0);
            }

            public Builder Clear()
            {
                ValueBuffer.Clear();
                ValidityBuffer.Clear();
                return this;
            }

            public Builder Reserve(int capacity)
            {
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity));
                }

                ValueBuffer.Reserve(capacity);
                ValidityBuffer.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                if (length < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(length));
                }

                ValueBuffer.Resize(length);
                ValidityBuffer.Resize(length);
                return this;
            }

            public Builder Toggle(int index)
            {
                CheckIndex(index);

                // If there is a null at this index, assume it was set to false in the value buffer, and so becomes
                // true/non-null after toggling.
                ValueBuffer.Toggle(index);
                ValidityBuffer.Set(index);
                return this;
            }

            public Builder Set(int index)
            {
                CheckIndex(index);
                ValueBuffer.Set(index);
                ValidityBuffer.Set(index);
                return this;
            }

            public Builder Set(int index, bool value)
            {
                CheckIndex(index);
                ValueBuffer.Set(index, value);
                ValidityBuffer.Set(index);
                return this;
            }

            public Builder Swap(int i, int j)
            {
                CheckIndex(i);
                CheckIndex(j);
                ValueBuffer.Swap(i, j);
                ValidityBuffer.Swap(i, j);
                return this;
            }

            private void CheckIndex(int index)
            {
                if (index < 0 || index >= Length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }
            }
        }

        public ArrowBuffer ValueBuffer => Data.Buffers[1];
        public ReadOnlySpan<byte> Values => ValueBuffer.Span.Slice(0, (int) Math.Ceiling(Length / 8.0));

        public BooleanArray(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(BooleanType.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public BooleanArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Boolean);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        [Obsolete("GetBoolean does not support null values. Use GetValue instead (which this method invokes internally).")]
        public bool GetBoolean(int index)
        {
            return GetValue(index).GetValueOrDefault();
        }

        public bool? GetValue(int index)
        {
            return IsNull(index)
                ? (bool?)null
                : BitUtility.GetBit(ValueBuffer.Span, index + Offset);
        }
    }
}
