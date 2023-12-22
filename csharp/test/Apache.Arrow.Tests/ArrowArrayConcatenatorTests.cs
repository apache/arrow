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
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowArrayConcatenatorTests
    {
        [Fact]
        public void TestStandardCases()
        {
            foreach ((List<IArrowArray> testTargetArrayList, IArrowArray expectedArray) in GenerateTestData())
            {
                IArrowArray actualArray = ArrowArrayConcatenator.Concatenate(testTargetArrayList);
                ArrowReaderVerifier.CompareArrays(expectedArray, actualArray);
            }
        }

        [Fact]
        public void TestNullOrEmpty()
        {
            Assert.Null(ArrowArrayConcatenator.Concatenate(null));
            Assert.Null(ArrowArrayConcatenator.Concatenate(new List<IArrowArray>()));
        }

        [Fact]
        public void TestSingleElement()
        {
            Int32Array array = new Int32Array.Builder().Append(1).Append(2).Build();
            IArrowArray actualArray = ArrowArrayConcatenator.Concatenate(new[] { array });
            ArrowReaderVerifier.CompareArrays(array, actualArray);
        }

        private static IEnumerable<Tuple<List<IArrowArray>, IArrowArray>> GenerateTestData()
        {
            var targetTypes = new List<IArrowType>() {
                    BooleanType.Default,
                    Int8Type.Default,
                    Int16Type.Default,
                    Int32Type.Default,
                    Int64Type.Default,
                    UInt8Type.Default,
                    UInt16Type.Default,
                    UInt32Type.Default,
                    UInt64Type.Default,
                    FloatType.Default,
                    DoubleType.Default,
                    BinaryType.Default,
                    BinaryViewType.Default,
                    StringType.Default,
                    StringViewType.Default,
                    Date32Type.Default,
                    Date64Type.Default,
                    TimestampType.Default,
                    new Decimal128Type(14, 10),
                    new Decimal256Type(14,10),
                    new ListType(Int64Type.Default),
                    new ListViewType(Int64Type.Default),
                    new StructType(new List<Field>{
                        new Field.Builder().Name("Strings").DataType(StringType.Default).Nullable(true).Build(),
                        new Field.Builder().Name("Ints").DataType(Int32Type.Default).Nullable(true).Build()
                    }),
                    new FixedSizeListType(Int32Type.Default, 1),
                    new UnionType(
                        new List<Field>{
                            new Field.Builder().Name("Strings").DataType(StringType.Default).Nullable(true).Build(),
                            new Field.Builder().Name("Ints").DataType(Int32Type.Default).Nullable(true).Build()
                        },
                        new[] { 0, 1 },
                        UnionMode.Sparse
                    ),
                    new UnionType(
                        new List<Field>{
                            new Field.Builder().Name("Strings").DataType(StringType.Default).Nullable(true).Build(),
                            new Field.Builder().Name("Ints").DataType(Int32Type.Default).Nullable(true).Build()
                        },
                        new[] { 0, 1 },
                        UnionMode.Dense
                    ),
                    new MapType(
                        new Field.Builder().Name("key").DataType(StringType.Default).Nullable(false).Build(),
                        new Field.Builder().Name("value").DataType(Int32Type.Default).Nullable(true).Build(),
                        keySorted: false),
                    IntervalType.YearMonth,
                    IntervalType.DayTime,
                    IntervalType.MonthDayNanosecond,
                };

            foreach (IArrowType type in targetTypes)
            {
                var creator = new TestDataGenerator();
                type.Accept(creator);
                yield return Tuple.Create(creator.TestTargetArrayList, creator.ExpectedArray);
            }
        }

        private class TestDataGenerator :
            IArrowTypeVisitor<BooleanType>,
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
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<BinaryViewType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<StringViewType>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<DurationType>,
            IArrowTypeVisitor<IntervalType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<ListViewType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<MapType>
        {

            private List<List<int?>> _baseData;

            private int _baseDataListCount;

            private int _baseDataTotalElementCount;

            public List<IArrowArray> TestTargetArrayList { get; }
            public IArrowArray ExpectedArray { get; private set; }

            public TestDataGenerator()
            {
                _baseData = new List<List<int?>> {
                    new List<int?> { 1, 2, 3 },
                    new List<int?> { 100, 101, null },
                    new List<int?> { 11, null, 12 },
                };

                _baseDataListCount = _baseData.Count;
                _baseDataTotalElementCount = _baseData.Sum(_ => _.Count);
                TestTargetArrayList = new List<IArrowArray>(_baseDataListCount);
            }

            public void Visit(BooleanType type) => GenerateTestData<bool, BooleanArray, BooleanArray.Builder>(type, x => x % 2 == 0);
            public void Visit(Int8Type type) => GenerateTestData<sbyte, Int8Array, Int8Array.Builder>(type, x => (sbyte)x);
            public void Visit(Int16Type type) => GenerateTestData<short, Int16Array, Int16Array.Builder>(type, x => (short)x);
            public void Visit(Int32Type type) => GenerateTestData<int, Int32Array, Int32Array.Builder>(type, x => x);
            public void Visit(Int64Type type) => GenerateTestData<long, Int64Array, Int64Array.Builder>(type, x => x);
            public void Visit(UInt8Type type) => GenerateTestData<byte, UInt8Array, UInt8Array.Builder>(type, x => (byte)x);
            public void Visit(UInt16Type type) => GenerateTestData<ushort, UInt16Array, UInt16Array.Builder>(type, x => (ushort)x);
            public void Visit(UInt32Type type) => GenerateTestData<uint, UInt32Array, UInt32Array.Builder>(type, x => (uint)x);
            public void Visit(UInt64Type type) => GenerateTestData<ulong, UInt64Array, UInt64Array.Builder>(type, x => (ulong)x);
            public void Visit(FloatType type) => GenerateTestData<float, FloatArray, FloatArray.Builder>(type, x => x);
            public void Visit(DoubleType type) => GenerateTestData<double, DoubleArray, DoubleArray.Builder>(type, x => x);
            public void Visit(Date32Type type) => GenerateTestData<DateTime, Date32Array, Date32Array.Builder>(type, x => DateTime.MinValue.AddDays(x));
            public void Visit(Date64Type type) => GenerateTestData<DateTime, Date64Array, Date64Array.Builder>(type, x => DateTime.MinValue.AddDays(x));

            public void Visit(Decimal128Type type)
            {
                Decimal128Array.Builder resultBuilder = new Decimal128Array.Builder(type).Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    Decimal128Array.Builder builder = new Decimal128Array.Builder(type).Reserve(dataList.Count);
                    foreach (decimal? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(value.Value);
                            resultBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(Decimal256Type type)
            {
                Decimal256Array.Builder resultBuilder = new Decimal256Array.Builder(type).Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    Decimal256Array.Builder builder = new Decimal256Array.Builder(type).Reserve(dataList.Count);
                    foreach (decimal? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(value.Value);
                            resultBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(TimestampType type)
            {
                TimestampArray.Builder resultBuilder = new TimestampArray.Builder().Reserve(_baseDataTotalElementCount);
                DateTimeOffset basis = DateTimeOffset.UtcNow;

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    TimestampArray.Builder builder = new TimestampArray.Builder().Reserve(dataList.Count);
                    foreach (int? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            DateTimeOffset dateValue = basis.AddMilliseconds(value.Value);
                            builder.Append(dateValue);
                            resultBuilder.Append(dateValue);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(DurationType type)
            {
                DurationArray.Builder resultBuilder = new DurationArray.Builder(type).Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    DurationArray.Builder builder = new DurationArray.Builder(type).Reserve(dataList.Count);
                    foreach (int? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(value.Value);
                            resultBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(IntervalType type)
            {
                switch (type.Unit)
                {
                    case IntervalUnit.YearMonth:
                        YearMonthIntervalArray.Builder yearMonthBuilder = new YearMonthIntervalArray.Builder().Reserve(_baseDataTotalElementCount);
                        foreach (List<int?> dataList in _baseData)
                        {
                            YearMonthIntervalArray.Builder yearMonthBuilder1 = new YearMonthIntervalArray.Builder().Reserve(dataList.Count);
                            foreach (int? value in dataList)
                            {
                                YearMonthInterval? ymi = value != null ? new YearMonthInterval(value.Value) : null;
                                yearMonthBuilder.Append(ymi);
                                yearMonthBuilder1.Append(ymi);
                            }
                            TestTargetArrayList.Add(yearMonthBuilder1.Build());
                        }
                        ExpectedArray = yearMonthBuilder.Build();
                        break;

                    case IntervalUnit.DayTime:
                        DayTimeIntervalArray.Builder dayTimeBuilder = new DayTimeIntervalArray.Builder().Reserve(_baseDataTotalElementCount);
                        foreach (List<int?> dataList in _baseData)
                        {
                            DayTimeIntervalArray.Builder dayTimeBuilder1 = new DayTimeIntervalArray.Builder().Reserve(dataList.Count);
                            foreach (int? value in dataList)
                            {
                                DayTimeInterval? dti = value != null ? new DayTimeInterval(100 - 50 * value.Value, 100 * value.Value) : null;
                                dayTimeBuilder.Append(dti);
                                dayTimeBuilder1.Append(dti);
                            }
                            TestTargetArrayList.Add(dayTimeBuilder1.Build());
                        }
                        ExpectedArray = dayTimeBuilder.Build();
                        break;

                    case IntervalUnit.MonthDayNanosecond:
                        MonthDayNanosecondIntervalArray.Builder monthDayNanoBuilder = new MonthDayNanosecondIntervalArray.Builder().Reserve(_baseDataTotalElementCount);
                        foreach (List<int?> dataList in _baseData)
                        {
                            MonthDayNanosecondIntervalArray.Builder monthDayNanoBuilder1 = new MonthDayNanosecondIntervalArray.Builder().Reserve(dataList.Count);
                            foreach (int? value in dataList)
                            {
                                MonthDayNanosecondInterval? mdni = value != null ? new MonthDayNanosecondInterval(value.Value, 5 - value.Value, 100 * value.Value) : null;
                                monthDayNanoBuilder.Append(mdni);
                                monthDayNanoBuilder1.Append(mdni);
                            }
                            TestTargetArrayList.Add(monthDayNanoBuilder1.Build());
                        }
                        ExpectedArray = monthDayNanoBuilder.Build();
                        break;

                    default:
                        throw new InvalidOperationException($"unsupported interval unit <{type.Unit}>");
                }
            }

            public void Visit(BinaryType type)
            {
                BinaryArray.Builder resultBuilder = new BinaryArray.Builder().Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    BinaryArray.Builder builder = new BinaryArray.Builder().Reserve(dataList.Count);

                    foreach (byte? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(value.Value);
                            resultBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(BinaryViewType type)
            {
                BinaryViewArray.Builder resultBuilder = new BinaryViewArray.Builder().Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    BinaryViewArray.Builder builder = new BinaryViewArray.Builder().Reserve(dataList.Count);

                    foreach (byte? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(value.Value);
                            resultBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(StringType type)
            {
                StringArray.Builder resultBuilder = new StringArray.Builder().Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    StringArray.Builder builder = new StringArray.Builder().Reserve(dataList.Count);

                    foreach (string value in dataList.Select(_ => _.ToString() ?? null))
                    {
                        builder.Append(value);
                        resultBuilder.Append(value);
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(StringViewType type)
            {
                StringViewArray.Builder resultBuilder = new StringViewArray.Builder().Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    StringViewArray.Builder builder = new StringViewArray.Builder().Reserve(dataList.Count);

                    foreach (string value in dataList.Select(_ => _.ToString() ?? null))
                    {
                        builder.Append(value);
                        resultBuilder.Append(value);
                    }
                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(ListType type)
            {
                ListArray.Builder resultBuilder = new ListArray.Builder(type.ValueDataType).Reserve(_baseDataTotalElementCount);
                Int64Array.Builder resultValueBuilder = (Int64Array.Builder)resultBuilder.ValueBuilder.Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];

                    ListArray.Builder builder = new ListArray.Builder(type.ValueField).Reserve(dataList.Count);
                    Int64Array.Builder valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(dataList.Count);

                    foreach (long? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append();
                            resultBuilder.Append();

                            valueBuilder.Append(value.Value);
                            resultValueBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }

                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(ListViewType type)
            {
                ListViewArray.Builder resultBuilder = new ListViewArray.Builder(type.ValueDataType).Reserve(_baseDataTotalElementCount);
                Int64Array.Builder resultValueBuilder = (Int64Array.Builder)resultBuilder.ValueBuilder.Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];

                    ListViewArray.Builder builder = new ListViewArray.Builder(type.ValueField).Reserve(dataList.Count);
                    Int64Array.Builder valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(dataList.Count);

                    foreach (long? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append();
                            resultBuilder.Append();

                            valueBuilder.Append(value.Value);
                            resultValueBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }

                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(FixedSizeListType type)
            {
                FixedSizeListArray.Builder resultBuilder = new FixedSizeListArray.Builder(type.ValueDataType, type.ListSize).Reserve(_baseDataTotalElementCount);
                Int32Array.Builder resultValueBuilder = (Int32Array.Builder)resultBuilder.ValueBuilder.Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];

                    FixedSizeListArray.Builder builder = new FixedSizeListArray.Builder(type.ValueField, type.ListSize).Reserve(dataList.Count);
                    Int32Array.Builder valueBuilder = (Int32Array.Builder)builder.ValueBuilder.Reserve(dataList.Count);

                    foreach (int? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append();
                            resultBuilder.Append();

                            valueBuilder.Append(value.Value);
                            resultValueBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }

                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(StructType type)
            {
                // TODO: Make data from type fields.

                // The following can be improved with a Builder class for StructArray.
                StringArray.Builder resultStringBuilder = new StringArray.Builder();
                Int32Array.Builder resultInt32Builder = new Int32Array.Builder();
                ArrowBuffer nullBitmapBuffer = new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(false).Build();

                for (int i = 0; i < 3; i++)
                {
                    resultStringBuilder.Append("joe").AppendNull().AppendNull().Append("mark");
                    resultInt32Builder.Append(1).Append(2).AppendNull().Append(4);
                    StringArray stringArray = new StringArray.Builder().Append("joe").AppendNull().AppendNull().Append("mark").Build();
                    Int32Array intArray = new Int32Array.Builder().Append(1).Append(2).AppendNull().Append(4).Build();
                    List<Array> arrays = new List<Array>
                    {
                        stringArray,
                        intArray
                    };

                    TestTargetArrayList.Add(new StructArray(type, 3, arrays, nullBitmapBuffer, 1));
                }

                StringArray resultStringArray = resultStringBuilder.Build();
                Int32Array resultInt32Array = resultInt32Builder.Build();

                ExpectedArray = new StructArray(type, 9, new List<Array> { resultStringArray, resultInt32Array }, nullBitmapBuffer, 3);
            }

            public void Visit(UnionType type)
            {
                bool isDense = type.Mode == UnionMode.Dense;

                StringArray.Builder stringResultBuilder = new StringArray.Builder().Reserve(_baseDataTotalElementCount);
                Int32Array.Builder intResultBuilder = new Int32Array.Builder().Reserve(_baseDataTotalElementCount);
                ArrowBuffer.Builder<byte> typeResultBuilder = new ArrowBuffer.Builder<byte>().Reserve(_baseDataTotalElementCount);
                ArrowBuffer.Builder<int> offsetResultBuilder = new ArrowBuffer.Builder<int>().Reserve(_baseDataTotalElementCount);
                int resultNullCount = 0;

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    StringArray.Builder stringBuilder = new StringArray.Builder().Reserve(dataList.Count);
                    Int32Array.Builder intBuilder = new Int32Array.Builder().Reserve(dataList.Count);
                    ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>().Reserve(dataList.Count);
                    ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>().Reserve(dataList.Count);
                    int nullCount = 0;

                    for (int j = 0; j < dataList.Count; j++)
                    {
                        byte index = (byte)Math.Max(j % 3, 1);
                        int? intValue = (index == 1) ? dataList[j] : null;
                        string stringValue = (index == 1) ? null : dataList[j]?.ToString();
                        typeBuilder.Append(index);

                        if (isDense)
                        {
                            if (index == 0)
                            {
                                offsetBuilder.Append(stringBuilder.Length);
                                offsetResultBuilder.Append(stringResultBuilder.Length);
                                stringBuilder.Append(stringValue);
                                stringResultBuilder.Append(stringValue);
                            }
                            else
                            {
                                offsetBuilder.Append(intBuilder.Length);
                                offsetResultBuilder.Append(intResultBuilder.Length);
                                intBuilder.Append(intValue);
                                intResultBuilder.Append(intValue);
                            }
                        }
                        else
                        {
                            stringBuilder.Append(stringValue);
                            stringResultBuilder.Append(stringValue);
                            intBuilder.Append(intValue);
                            intResultBuilder.Append(intValue);
                        }

                        if (dataList[j] == null)
                        {
                            nullCount++;
                            resultNullCount++;
                        }
                    }

                    ArrowBuffer[] buffers;
                    if (isDense)
                    {
                        buffers = new[] { typeBuilder.Build(), offsetBuilder.Build() };
                    }
                    else
                    {
                        buffers = new[] { typeBuilder.Build() };
                    }
                    TestTargetArrayList.Add(UnionArray.Create(new ArrayData(
                        type, dataList.Count, nullCount, 0, buffers,
                        new[] { stringBuilder.Build().Data, intBuilder.Build().Data })));
                }

                ArrowBuffer[] resultBuffers;
                if (isDense)
                {
                    resultBuffers = new[] { typeResultBuilder.Build(), offsetResultBuilder.Build() };
                }
                else
                {
                    resultBuffers = new[] { typeResultBuilder.Build() };
                }
                ExpectedArray = UnionArray.Create(new ArrayData(
                    type, _baseDataTotalElementCount, resultNullCount, 0, resultBuffers,
                        new[] { stringResultBuilder.Build().Data, intResultBuilder.Build().Data }));
            }

            public void Visit(MapType type)
            {
                MapArray.Builder resultBuilder = new MapArray.Builder(type).Reserve(_baseDataTotalElementCount);
                StringArray.Builder resultKeyBuilder = (StringArray.Builder)resultBuilder.KeyBuilder.Reserve(_baseDataTotalElementCount);
                Int32Array.Builder resultValueBuilder = (Int32Array.Builder)resultBuilder.ValueBuilder.Reserve(_baseDataTotalElementCount);
                ArrowBuffer nullBitmapBuilder = new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(false).Build();

                for (int i = 0; i < _baseData.Count; i++)
                {
                    List<int?> dataList = _baseData[i];

                    MapArray.Builder builder = new MapArray.Builder(type).Reserve(dataList.Count);
                    StringArray.Builder keyBuilder = (StringArray.Builder)builder.KeyBuilder.Reserve(dataList.Count);
                    Int32Array.Builder valueBuilder = (Int32Array.Builder)builder.ValueBuilder.Reserve(dataList.Count);

                    foreach (int? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append();
                            resultBuilder.Append();

                            keyBuilder.Append(value.Value.ToString());
                            valueBuilder.Append(value.Value);
                            resultKeyBuilder.Append(value.Value.ToString());
                            resultValueBuilder.Append(value.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }

                    TestTargetArrayList.Add(builder.Build());
                }

                ExpectedArray = resultBuilder.Build();
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }

            private void GenerateTestData<T, TArray, TArrayBuilder>(IArrowType type, Func<int, T> generator)
                where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>
                where TArray : IArrowArray
            {
                var resultBuilder = (IArrowArrayBuilder<T, TArray, TArrayBuilder>)ArrayArrayBuilderFactoryReflector.InvokeBuild(type);
                resultBuilder.Reserve(_baseDataTotalElementCount);

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    var builder = (IArrowArrayBuilder<T, TArray, TArrayBuilder>)ArrayArrayBuilderFactoryReflector.InvokeBuild(type);
                    builder.Reserve(dataList.Count);

                    foreach (int? value in dataList)
                    {
                        if (value.HasValue)
                        {
                            builder.Append(generator(value.Value));
                            resultBuilder.Append(generator(value.Value));
                        }
                        else
                        {
                            builder.AppendNull();
                            resultBuilder.AppendNull();
                        }
                    }
                    TestTargetArrayList.Add(builder.Build(default));
                }

                ExpectedArray = resultBuilder.Build(default);
            }
        }
    }
}
