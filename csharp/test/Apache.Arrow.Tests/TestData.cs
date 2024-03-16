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

using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Tests
{
    public static class TestData
    {
        public static RecordBatch CreateSampleRecordBatch(int length, bool createDictionaryArray)
        {
            HashSet<ArrowTypeId> excluded = createDictionaryArray ? null : new HashSet<ArrowTypeId> { ArrowTypeId.Dictionary };
            return CreateSampleRecordBatch(length, columnSetCount: 1, excluded);
        }

        public static RecordBatch CreateSampleRecordBatch(
            int length,
            int columnSetCount = 1,
            HashSet<ArrowTypeId> excludedTypes = null)
        {
            Schema.Builder builder = new Schema.Builder();

            void AddField(Field field)
            {
                if (excludedTypes == null || !excludedTypes.Contains(field.DataType.TypeId))
                {
                    builder.Field(field);
                }
            }

            for (int i = 0; i < columnSetCount; i++)
            {
                AddField(CreateField(new ListType(Int64Type.Default), i));
                AddField(CreateField(new ListViewType(Int64Type.Default), i));
                AddField(CreateField(BooleanType.Default, i));
                AddField(CreateField(UInt8Type.Default, i));
                AddField(CreateField(Int8Type.Default, i));
                AddField(CreateField(UInt16Type.Default, i));
                AddField(CreateField(Int16Type.Default, i));
                AddField(CreateField(UInt32Type.Default, i));
                AddField(CreateField(Int32Type.Default, i));
                AddField(CreateField(UInt64Type.Default, i));
                AddField(CreateField(Int64Type.Default, i));
#if NET5_0_OR_GREATER
                AddField(CreateField(HalfFloatType.Default, i));
#endif
                AddField(CreateField(FloatType.Default, i));
                AddField(CreateField(DoubleType.Default, i));
                AddField(CreateField(Date32Type.Default, i));
                AddField(CreateField(Date64Type.Default, i));
                AddField(CreateField(Time32Type.Default, i));
                AddField(CreateField(Time64Type.Default, i));
                AddField(CreateField(TimestampType.Default, i));
                AddField(CreateField(StringType.Default, i));
                AddField(CreateField(StringViewType.Default, i));
                AddField(CreateField(new StructType(new List<Field> { CreateField(StringType.Default, i), CreateField(Int32Type.Default, i) }), i));
                AddField(CreateField(new Decimal128Type(10, 6), i));
                AddField(CreateField(new Decimal256Type(16, 8), i));
                AddField(CreateField(new MapType(StringType.Default, Int32Type.Default), i));
                AddField(CreateField(IntervalType.YearMonth, i));
                AddField(CreateField(IntervalType.DayTime, i));
                AddField(CreateField(IntervalType.MonthDayNanosecond, i));
                AddField(CreateField(BinaryType.Default, i));
                AddField(CreateField(BinaryViewType.Default, i));
                AddField(CreateField(new FixedSizeBinaryType(16), i));
                AddField(CreateField(new FixedSizeListType(Int32Type.Default, 3), i));
                AddField(CreateField(new UnionType(new[] { CreateField(StringType.Default, i), CreateField(Int32Type.Default, i) }, new[] { 0, 1 }, UnionMode.Sparse), i));
                AddField(CreateField(new UnionType(new[] { CreateField(StringType.Default, i), CreateField(Int32Type.Default, i) }, new[] { 0, 1 }, UnionMode.Dense), -i));
                AddField(CreateField(new DictionaryType(Int32Type.Default, StringType.Default, false), i));
            }

            Schema schema = builder.Build();

            return CreateSampleRecordBatch(schema, length);
        }

        public static RecordBatch CreateSampleRecordBatch(Schema schema, int length)
        {
            IEnumerable<IArrowArray> arrays = CreateArrays(schema, length);

            return new RecordBatch(schema, arrays, length);
        }

        private static Field CreateField(ArrowType type, int iteration)
        {
            return new Field(type.Name + iteration, type, nullable: false);
        }

        public static IEnumerable<IArrowArray> CreateArrays(Schema schema, int length)
        {
            int fieldCount = schema.FieldsList.Count;
            List<IArrowArray> arrays = new List<IArrowArray>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                Field field = schema.GetFieldByIndex(i);
                arrays.Add(CreateArray(field, length));
            }
            return arrays;
        }

        private static IArrowArray CreateArray(Field field, int length)
        {
            var creator = new ArrayCreator(length);

            field.DataType.Accept(creator);

            return creator.Array;
        }

        private class ArrayCreator :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<Time32Type>,
            IArrowTypeVisitor<Time64Type>,
            IArrowTypeVisitor<DurationType>,
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<StringViewType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<ListViewType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<DictionaryType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<BinaryViewType>,
            IArrowTypeVisitor<FixedSizeBinaryType>,
            IArrowTypeVisitor<MapType>,
            IArrowTypeVisitor<IntervalType>,
#if NET5_0_OR_GREATER
            IArrowTypeVisitor<HalfFloatType>,
#endif
            IArrowTypeVisitor<NullType>
        {
            private int Length { get; }
            public IArrowArray Array { get; private set; }

            public ArrayCreator(int length)
            {
                Length = length;
            }

            public void Visit(BooleanType type) => GenerateArray(new BooleanArray.Builder(), x => x % 2 == 0);
            public void Visit(Int8Type type) => GenerateArray(new Int8Array.Builder(), x => (sbyte)x);
            public void Visit(Int16Type type) => GenerateArray(new Int16Array.Builder(), x => (short)x);
            public void Visit(Int32Type type) => GenerateArray(new Int32Array.Builder(), x => x);
            public void Visit(Int64Type type) => GenerateArray(new Int64Array.Builder(), x => x);
            public void Visit(UInt8Type type) => GenerateArray(new UInt8Array.Builder(), x => (byte)x);
            public void Visit(UInt16Type type) => GenerateArray(new UInt16Array.Builder(), x => (ushort)x);
            public void Visit(UInt32Type type) => GenerateArray(new UInt32Array.Builder(), x => (uint)x);
            public void Visit(UInt64Type type) => GenerateArray(new UInt64Array.Builder(), x => (ulong)x);
            public void Visit(FloatType type) => GenerateArray(new FloatArray.Builder(), x => ((float)x / Length));
#if NET5_0_OR_GREATER
            public void Visit(HalfFloatType type) => GenerateArray(new HalfFloatArray.Builder(), x => ((Half)x / (Half)Length));
#endif
            public void Visit(DoubleType type) => GenerateArray(new DoubleArray.Builder(), x => ((double)x / Length));
            public void Visit(Decimal128Type type)
            {
                var builder = new Decimal128Array.Builder(type).Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append((decimal)i / Length);
                }

                Array = builder.Build();
            }

            public void Visit(Decimal256Type type)
            {
                var builder = new Decimal256Array.Builder(type).Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append((decimal)i / Length);
                }

                Array = builder.Build();
            }

            public void Visit(Date32Type type)
            {
                var builder = new Date32Array.Builder().Reserve(Length);

                // Length can be greater than the number of days since DateTime.MinValue.
                // Set a cap for how many days can be subtracted from now.
                int maxDays = Math.Min(Length, 100_000);
                var basis = DateTimeOffset.UtcNow.AddDays(-maxDays);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddDays(i % maxDays));
                }

                Array = builder.Build();
            }

            public void Visit(Date64Type type)
            {
                var builder = new Date64Array.Builder().Reserve(Length);
                var basis = DateTimeOffset.UtcNow.AddSeconds(-Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddSeconds(i));
                }

                Array = builder.Build();
            }

            public void Visit(Time32Type type)
            {
                var builder = new Time32Array.Builder(type).Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(i);
                }

                Array = builder.Build();
            }

            public void Visit(Time64Type type)
            {
                var builder = new Time64Array.Builder(type).Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(i);
                }

                Array = builder.Build();
            }

            public void Visit(DurationType type)
            {
                var builder = new DurationArray.Builder(type).Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(i);
                }

                Array = builder.Build();
            }

            public void Visit(TimestampType type)
            {
                var builder = new TimestampArray.Builder().Reserve(Length);
                var basis = DateTimeOffset.UtcNow.AddMilliseconds(-Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddMilliseconds(i));
                }

                Array = builder.Build();
            }

            public void Visit(StringType type)
            {
                var str = "hello";
                var builder = new StringArray.Builder();

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(str);
                }

                Array = builder.Build();
            }

            public void Visit(StringViewType type)
            {
                var str = "length=ten";
                var builder = new StringViewArray.Builder();

                for (var i = 0; i < Length; i++)
                {
                    switch (i % 3)
                    {
                        case 0:
                            builder.AppendNull();
                            break;
                        case 1:
                            builder.Append(str);
                            break;
                        case 2:
                            builder.Append(str + str);
                            break;
                    }
                }

                Array = builder.Build();
            }

            public void Visit(ListType type)
            {
                var builder = new ListArray.Builder(type.ValueField).Reserve(Length);

                var valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(Length + 1);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append();
                    valueBuilder.Append(i);
                }
                //Add a value to check if Values.Length can exceed ListArray.Length
                valueBuilder.Append(0);

                Array = builder.Build();
            }

            public void Visit(ListViewType type)
            {
                var builder = new ListViewArray.Builder(type.ValueField).Reserve(Length);

                var valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(Length + 1);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append();
                    valueBuilder.Append(i);
                }
                //Add a value to check if Values.Length can exceed ListArray.Length
                valueBuilder.Append(0);

                Array = builder.Build();
            }

            public void Visit(FixedSizeListType type)
            {
                var builder = new FixedSizeListArray.Builder(type.ValueField, type.ListSize).Reserve(Length);

                var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

                for (var i = 0; i < Length; i++)
                {
                    if (type.Fields[0].IsNullable && (i % 3) == 0)
                    {
                        builder.AppendNull();
                    }
                    else
                    {
                        builder.Append();
                        for (var j = 0; j < type.ListSize; j++)
                        {
                            valueBuilder.Append(i * type.ListSize + j);
                        }
                    }
                }

                Array = builder.Build();
            }

            public void Visit(StructType type)
            {
                IArrowArray[] childArrays = new IArrowArray[type.Fields.Count];
                for (int i = 0; i < childArrays.Length; i++)
                {
                    childArrays[i] = CreateArray(type.Fields[i], Length);
                }

                ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
                for (int i = 0; i < Length; i++)
                {
                    nullBitmap.Append(true);
                }

                Array = new StructArray(type, Length, childArrays, nullBitmap.Build());
            }

            public void Visit(UnionType type)
            {
                int[] lengths = new int[type.Fields.Count];
                if (type.Mode == UnionMode.Sparse)
                {
                    for (int i = 0; i < lengths.Length; i++)
                    {
                        lengths[i] = Length;
                    }
                }
                else
                {
                    int totalLength = Length;
                    int oneLength = Length / lengths.Length;
                    for (int i = 1; i < lengths.Length; i++)
                    {
                        lengths[i] = oneLength;
                        totalLength -= oneLength;
                    }
                    lengths[0] = totalLength;
                }

                ArrayData[] childArrays = new ArrayData[type.Fields.Count];
                for (int i = 0; i < childArrays.Length; i++)
                {
                    childArrays[i] = CreateArray(type.Fields[i], lengths[i]).Data;
                }

                ArrowBuffer.Builder<byte> typeIdBuilder = new ArrowBuffer.Builder<byte>(Length);
                byte index = 0;
                for (int i = 0; i < Length; i++)
                {
                    typeIdBuilder.Append(index);
                    index++;
                    if (index == lengths.Length)
                    {
                        index = 0;
                    }
                }

                ArrowBuffer[] buffers;
                if (type.Mode == UnionMode.Sparse)
                {
                    buffers = new ArrowBuffer[1];
                }
                else
                {
                    ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>(Length);
                    for (int i = 0; i < Length; i++)
                    {
                        offsetBuilder.Append(i / lengths.Length);
                    }

                    buffers = new ArrowBuffer[2];
                    buffers[1] = offsetBuilder.Build();
                }
                buffers[0] = typeIdBuilder.Build();

                Array = UnionArray.Create(new ArrayData(type, Length, 0, 0, buffers, childArrays));
            }

            public void Visit(DictionaryType type)
            {
                Int32Array.Builder indicesBuilder = new Int32Array.Builder().Reserve(Length);
                StringArray.Builder valueBuilder = new StringArray.Builder().Reserve(Length);

                for (int i = 0; i < Length; i++)
                {
                    indicesBuilder.Append(i);
                    valueBuilder.Append($"{i}");
                }

                Array = new DictionaryArray(type, indicesBuilder.Build(), valueBuilder.Build());
            }

            public void Visit(BinaryType type)
            {
                ReadOnlySpan<byte> shortData = new[] { (byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9 };
                ReadOnlySpan<byte> longData = new[]
                {
                    (byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9,
                    (byte)10, (byte)11, (byte)12, (byte)13, (byte)14, (byte)15, (byte)16, (byte)17, (byte)18, (byte)19
                };
                var builder = new BinaryArray.Builder();

                for (var i = 0; i < Length; i++)
                {
                    switch (i % 3)
                    {
                        case 0:
                            builder.AppendNull();
                            break;
                        case 1:
                            builder.Append(shortData);
                            break;
                        case 2:
                            builder.Append(longData);
                            break;
                    }
                }

                Array = builder.Build();
            }

            public void Visit(BinaryViewType type)
            {
                ReadOnlySpan<byte> shortData = new[] { (byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9 };
                ReadOnlySpan<byte> longData = new[]
                {
                    (byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9,
                    (byte)10, (byte)11, (byte)12, (byte)13, (byte)14, (byte)15, (byte)16, (byte)17, (byte)18, (byte)19
                };
                var builder = new BinaryViewArray.Builder();

                for (var i = 0; i < Length; i++)
                {
                    switch (i % 3)
                    {
                        case 0:
                            builder.AppendNull();
                            break;
                        case 1:
                            builder.Append(shortData);
                            break;
                        case 2:
                            builder.Append(longData);
                            break;
                    }
                }

                Array = builder.Build();
            }

            public void Visit(FixedSizeBinaryType type)
            {
                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();

                int valueSize = type.BitWidth;
                for (int i = 0; i < Length; i++)
                {
                    valueBuilder.Append(Enumerable.Repeat((byte)i, valueSize).ToArray());
                }

                ArrowBuffer validityBuffer = ArrowBuffer.Empty;
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                ArrayData arrayData = new ArrayData(type, Length, 0, 0, new[] { validityBuffer, valueBuffer });
                Array = new FixedSizeBinaryArray(arrayData);
            }

            public void Visit(MapType type)
            {
                MapArray.Builder builder = new MapArray.Builder(type).Reserve(Length);
                var keyBuilder = builder.KeyBuilder.Reserve(Length + 1) as StringArray.Builder;
                var valueBuilder = builder.ValueBuilder.Reserve(Length + 1) as Int32Array.Builder;

                for (var i = 0; i < Length; i++)
                {
                    builder.Append();
                    keyBuilder.Append(i.ToString());
                    valueBuilder.Append(i);
                }
                //Add a value to check if Values.Length can exceed MapArray.Length
                keyBuilder.Append("0");
                valueBuilder.Append(0);

                Array = builder.Build();
            }

            public void Visit(IntervalType type)
            {
                switch (type.Unit)
                {
                    case IntervalUnit.YearMonth:
                        var yearMonthBuilder = new YearMonthIntervalArray.Builder().Reserve(Length);
                        for (var i = 0; i < Length; i++)
                        {
                            yearMonthBuilder.Append(new YearMonthInterval(i));
                        }
                        Array = yearMonthBuilder.Build();
                        break;
                    case IntervalUnit.DayTime:
                        var dayTimeBuilder = new DayTimeIntervalArray.Builder().Reserve(Length);
                        for (var i = 0; i < Length; i++)
                        {
                            dayTimeBuilder.Append(new DayTimeInterval(100 - 50*i, 100 * i));
                        }
                        Array = dayTimeBuilder.Build();
                        break;
                    case IntervalUnit.MonthDayNanosecond:
                        var monthDayNanoBuilder = new MonthDayNanosecondIntervalArray.Builder().Reserve(Length);
                        for (var i = 0; i < Length; i++)
                        {
                            monthDayNanoBuilder.Append(new MonthDayNanosecondInterval(i, 5-i, 100*i));
                        }
                        Array = monthDayNanoBuilder.Build();
                        break;
                    default:
                        throw new InvalidOperationException($"unsupported interval unit <{type.Unit}>");
                }
            }

            public void Visit(NullType type)
            {
                Array = new NullArray(Length);
            }

            private void GenerateArray<T, TArray, TArrayBuilder>(IArrowArrayBuilder<T, TArray, TArrayBuilder> builder, Func<int, T> generator)
                where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>
                where TArray : IArrowArray
                where T : struct
            {
                for (var i = 0; i < Length; i++)
                {
                    if (i == Length - 2)
                    {
                        builder.AppendNull();
                    }
                    else
                    {
                        var value = generator(i);
                        builder.Append(value);
                    }
                }

                Array = builder.Build(default);
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }
        }
    }
}
