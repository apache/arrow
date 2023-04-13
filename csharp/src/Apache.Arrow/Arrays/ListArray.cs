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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class ListArray : Array, IEnumerable<IArrowArray>
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

            public Builder Append<T>(IEnumerable<T> values, System.Text.Encoding encoding = null)
            {
                return AppendRange(new List<IEnumerable<T>> { values }, encoding);
            }

            public Builder AppendRange<T>(IEnumerable<IEnumerable<T>> values, System.Text.Encoding encoding = null)
            {
                var type = DataType as ListType;

                switch (type.ValueDataType.TypeId)
                {
                    case ArrowTypeId.Binary:
                        var binBuilder = ValueBuilder as BinaryArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                binBuilder.AppendRange(value as IEnumerable<byte[]>);
                            }
                        }
                        break;
                    case ArrowTypeId.String:
                        var strBuilder = ValueBuilder as StringArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                strBuilder.AppendRange(value as IEnumerable<string>, encoding);
                            }
                        }
                        break;
                    case ArrowTypeId.Int16:
                        var int16Builder = ValueBuilder as Int16Array.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                int16Builder.AppendRange(value as IEnumerable<short>);
                            }
                        }
                        break;
                    case ArrowTypeId.Int32:
                        var int32Builder = ValueBuilder as Int32Array.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                int32Builder.AppendRange(value as IEnumerable<int>);
                            }
                        }
                        break;
                    case ArrowTypeId.Int64:
                        var int64Builder = ValueBuilder as Int64Array.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                int64Builder.AppendRange(value as IEnumerable<long>);
                            }
                        }
                        break;
                    case ArrowTypeId.Double:
                        var doubleBuilder = ValueBuilder as DoubleArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                doubleBuilder.AppendRange(value as IEnumerable<double>);
                            }
                        }
                        break;
                    case ArrowTypeId.Decimal128:
                        var d128Builder = ValueBuilder as Decimal128Array.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                d128Builder.AppendRange(value as IEnumerable<decimal>);
                            }
                        }
                        break;
                    case ArrowTypeId.Decimal256:
                        var d256Builder = ValueBuilder as Decimal256Array.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                d256Builder.AppendRange(value as IEnumerable<decimal>);
                            }
                        }
                        break;
                    case ArrowTypeId.Boolean:
                        var boolBuilder = ValueBuilder as BooleanArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                boolBuilder.AppendRange(value as IEnumerable<bool>);
                            }
                        }
                        break;
                    case ArrowTypeId.Timestamp:
                        var tsBuilder = ValueBuilder as TimestampArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                tsBuilder.AppendRange(value as IEnumerable<DateTimeOffset>);
                            }
                        }
                        break;
                    case ArrowTypeId.Time64:
                        var t64Builder = ValueBuilder as Time64Array.Builder;
                        var t64Type = (Time64Type)DataType;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                t64Builder.AppendRange((value as IEnumerable<TimeSpan>).Select(t64Type.ToLong));
                            }
                        }
                        break;
                    case ArrowTypeId.Time32:
                        var t32Builder = ValueBuilder as Time32Array.Builder;
                        var t32Type = (Time32Type)DataType;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                t32Builder.AppendRange((value as IEnumerable<TimeSpan>).Select(t32Type.ToInt));
                            }
                        }
                        break;
                    case ArrowTypeId.List:
                        var listBuilder = ValueBuilder as ListArray.Builder;

                        foreach (IEnumerable<T> value in values)
                        {
                            if (value == null)
                            {
                                AppendNull();
                            }
                            else
                            {
                                Append();
                                listBuilder.AppendRange(value as IEnumerable<IEnumerable<T>>, encoding);
                            }
                        }
                        break;
                    default:
                        throw new NotImplementedException($"Cannot AppendRange with {typeof(T)}, need to do it manually with this.Append()");
                }

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
                ValidityBufferBuilder.Reserve(capacity + 1);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length + 1);
                ValidityBufferBuilder.Resize(length + 1);
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

        // IEnumerable methods
        public IEnumerator<IArrowArray> GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private new class Enumerator : Array.Enumerator<ListArray>, IEnumerator<IArrowArray>
        {
            public Enumerator(ListArray array) : base(array)
            { }

            IArrowArray IEnumerator<IArrowArray>.Current => Array.GetSlicedValues(Position);

            object IEnumerator.Current => Array.GetSlicedValues(Position);
        }
    }
}
