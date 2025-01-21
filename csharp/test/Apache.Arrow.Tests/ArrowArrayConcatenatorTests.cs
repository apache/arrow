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
        public void TestConcatenateSlices()
        {
            foreach ((List<IArrowArray> testTargetArrayList, IArrowArray expectedArray) in GenerateTestData(slicedArrays: true))
            {
                IArrowArray actualArray = ArrowArrayConcatenator.Concatenate(testTargetArrayList);
                ArrowReaderVerifier.CompareArrays(expectedArray, actualArray, strictCompare: false);
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

        private static IEnumerable<Tuple<List<IArrowArray>, IArrowArray>> GenerateTestData(bool slicedArrays = false)
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
                    new Decimal32Type(7, 3),
                    new Decimal64Type(14, 4),
                    new Decimal128Type(14, 10),
                    new Decimal256Type(14, 10),
                    new ListType(Int64Type.Default),
                    new ListViewType(Int64Type.Default),
                    new StructType(new List<Field>{
                        new Field.Builder().Name("Strings").DataType(StringType.Default).Nullable(true).Build(),
                        new Field.Builder().Name("Ints").DataType(Int32Type.Default).Nullable(true).Build()
                    }),
                    new FixedSizeListType(Int32Type.Default, 2),
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
                var creator = new TestDataGenerator(slicedArrays);
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
            IArrowTypeVisitor<Decimal32Type>,
            IArrowTypeVisitor<Decimal64Type>,
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
            private readonly List<List<int?>> _baseData;

            private readonly int _baseDataListCount;

            private readonly int _resultTotalElementCount;

            private readonly List<(int Offset, int Length)> _sliceParameters = null;

            public List<IArrowArray> TestTargetArrayList { get; }
            public IArrowArray ExpectedArray { get; private set; }

            public TestDataGenerator(bool slicedArrays)
            {
                _baseData = new List<List<int?>> {
                    new List<int?> { 1, 2, 3, 4, 5, 6 },
                    new List<int?> { 100, 101, null, 102, null, 103 },
                    new List<int?> { null, null },
                    new List<int?> { },
                    new List<int?> { 11, null, 12, 13, 14 },
                };

                if (slicedArrays)
                {
                    _sliceParameters = new List<(int, int)>
                    {
                        (2, 3),
                        (0, 5),
                        (0, 2),
                        (0, 0),
                        (1, 4),
                    };
                }

                _baseDataListCount = _baseData.Count;
                _resultTotalElementCount = slicedArrays
                    ? _sliceParameters.Sum(p => p.Length)
                    : _baseData.Sum(baseList => baseList.Count);

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

            public void Visit(Decimal32Type type) => GenerateTestData<Decimal32Array, Decimal32Array.Builder>(type, (builder, x) => builder.Append(x));
            public void Visit(Decimal64Type type) => GenerateTestData<Decimal64Array, Decimal64Array.Builder>(type, (builder, x) => builder.Append(x));
            public void Visit(Decimal128Type type) => GenerateTestData<Decimal128Array, Decimal128Array.Builder>(type, (builder, x) => builder.Append(x));
            public void Visit(Decimal256Type type) => GenerateTestData<Decimal256Array, Decimal256Array.Builder>(type, (builder, x) => builder.Append(x));

            public void Visit(TimestampType type)
            {
                DateTimeOffset basis = DateTimeOffset.UtcNow;
                GenerateTestData<TimestampArray, TimestampArray.Builder>(type, (builder, x) => builder.Append(basis.AddMilliseconds(x)));
            }

            public void Visit(DurationType type) => GenerateTestData<long, DurationArray, DurationArray.Builder>(type, x => (long)x);

            public void Visit(IntervalType type)
            {
                switch (type.Unit)
                {
                    case IntervalUnit.YearMonth:
                        GenerateTestData<YearMonthInterval, YearMonthIntervalArray, YearMonthIntervalArray.Builder>(
                            type, x => new YearMonthInterval(x));
                        break;

                    case IntervalUnit.DayTime:
                        GenerateTestData<DayTimeInterval, DayTimeIntervalArray, DayTimeIntervalArray.Builder>(
                            type, x => new DayTimeInterval(100 - 50 * x, 100 * x));
                        break;

                    case IntervalUnit.MonthDayNanosecond:
                        GenerateTestData<MonthDayNanosecondInterval, MonthDayNanosecondIntervalArray, MonthDayNanosecondIntervalArray.Builder>(
                            type, x => new MonthDayNanosecondInterval(x, 5 - x, 100 * x));
                        break;

                    default:
                        throw new InvalidOperationException($"unsupported interval unit <{type.Unit}>");
                }
            }

            public void Visit(BinaryType type) =>
                GenerateTestData<BinaryArray, BinaryArray.Builder>(type, (builder, x) =>
                {
                    if (x % 2 == 0)
                    {
                        builder.Append((byte)x);
                    }
                    else
                    {
                        builder.Append(new byte[] {(byte)x, (byte)(x + 1)}.AsSpan());
                    }
                });

            public void Visit(BinaryViewType type) =>
                GenerateTestData<BinaryViewArray, BinaryViewArray.Builder>(type, (builder, x) =>
                {
                    if (x % 2 == 0)
                    {
                        builder.Append((byte)x);
                    }
                    else
                    {
                        builder.Append(new byte[] {(byte)x, (byte)(x + 1)}.AsSpan());
                    }
                });

            public void Visit(StringType type) =>
                GenerateTestData<StringArray, StringArray.Builder>(type, (builder, x) => builder.Append(x.ToString()));

            public void Visit(StringViewType type) =>
                GenerateTestData<StringViewArray, StringViewArray.Builder>(type, (builder, x) => builder.Append(x.ToString()));

            public void Visit(ListType type) =>
                GenerateTestData<ListArray, ListArray.Builder>(type, (builder, x) =>
                {
                    builder.Append();
                    ((Int64Array.Builder)builder.ValueBuilder).Append(x);
                }, initAction: (builder, length) =>
                {
                    builder.Reserve(length);
                    builder.ValueBuilder.Reserve(length);
                });

            public void Visit(ListViewType type) =>
                GenerateTestData<ListViewArray, ListViewArray.Builder>(type, (builder, x) =>
                {
                    builder.Append();
                    ((Int64Array.Builder)builder.ValueBuilder).Append(x);
                }, initAction: (builder, length) =>
                {
                    builder.Reserve(length);
                    builder.ValueBuilder.Reserve(length);
                });

            public void Visit(FixedSizeListType type) =>
                GenerateTestData<FixedSizeListArray, FixedSizeListArray.Builder>(type, (builder, x) =>
                {
                    builder.Append();
                    var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;
                    for (int i = 0; i < type.ListSize; ++i)
                    {
                        valueBuilder.Append(x);
                    }
                }, initAction: (builder, length) =>
                {
                    builder.Reserve(length);
                    builder.ValueBuilder.Reserve(length * type.ListSize);
                });

            public void Visit(StructType type)
            {
                // TODO: Make data from type fields.

                // The following can be improved with a Builder class for StructArray.
                StringArray.Builder resultStringBuilder = new StringArray.Builder().Reserve(_resultTotalElementCount);
                Int32Array.Builder resultInt32Builder = new Int32Array.Builder().Reserve(_resultTotalElementCount);
                ArrowBuffer.BitmapBuilder resultNullBitmapBuilder = new ArrowBuffer.BitmapBuilder().Reserve(_resultTotalElementCount);
                int resultNullCount = 0;

                for (int i = 0; i < _baseData.Count; i++)
                {
                    List<int?> dataList = _baseData[i];
                    StringArray.Builder stringBuilder = new StringArray.Builder().Reserve(dataList.Count);
                    Int32Array.Builder int32Builder = new Int32Array.Builder().Reserve(dataList.Count);
                    ArrowBuffer.BitmapBuilder nullBitmapBuilder = new ArrowBuffer.BitmapBuilder().Reserve(dataList.Count);
                    int nullCount = 0;

                    for (int j = 0; j < dataList.Count; ++j)
                    {
                        var value = dataList[j];
                        if (value.HasValue)
                        {
                            nullBitmapBuilder.Append(true);
                            stringBuilder.Append(value.Value.ToString());
                            int32Builder.Append(value.Value);

                            if (IncludeInResult(i, j))
                            {
                                resultNullBitmapBuilder.Append(true);
                                resultStringBuilder.Append(value.Value.ToString());
                                resultInt32Builder.Append(value.Value);
                            }
                        }
                        else
                        {
                            nullCount++;
                            nullBitmapBuilder.Append(false);
                            stringBuilder.Append("");
                            int32Builder.Append(0);

                            if (IncludeInResult(i, j))
                            {
                                resultNullCount++;
                                resultNullBitmapBuilder.Append(false);
                                resultStringBuilder.Append("");
                                resultInt32Builder.Append(0);
                            }
                        }
                    }

                    var arrays = new List<Array>
                    {
                        stringBuilder.Build(),
                        int32Builder.Build(),
                    };

                    TestTargetArrayList.Add(SliceTargetArray(
                        new StructArray(type, dataList.Count, arrays, nullBitmapBuilder.Build(), nullCount), i));
                }

                var resultArrays = new List<Array>
                {
                    resultStringBuilder.Build(),
                    resultInt32Builder.Build(),
                };

                ExpectedArray = new StructArray(
                    type, _resultTotalElementCount, resultArrays, resultNullBitmapBuilder.Build(), resultNullCount);
            }

            public void Visit(UnionType type)
            {
                bool isDense = type.Mode == UnionMode.Dense;

                StringArray.Builder stringResultBuilder = new StringArray.Builder().Reserve(_resultTotalElementCount);
                Int32Array.Builder intResultBuilder = new Int32Array.Builder().Reserve(_resultTotalElementCount);
                ArrowBuffer.Builder<byte> typeResultBuilder = new ArrowBuffer.Builder<byte>().Reserve(_resultTotalElementCount);
                ArrowBuffer.Builder<int> offsetResultBuilder = new ArrowBuffer.Builder<int>().Reserve(_resultTotalElementCount);
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
                        bool includeInResult = IncludeInResult(i, j);
                        byte index = (byte)Math.Min(j % 3, 1);
                        int? intValue = (index == 1) ? dataList[j] : null;
                        string stringValue = (index == 1) ? null : dataList[j]?.ToString();
                        typeBuilder.Append(index);
                        if (includeInResult)
                        {
                            typeResultBuilder.Append(index);
                        }

                        if (isDense)
                        {
                            if (index == 0)
                            {
                                offsetBuilder.Append(stringBuilder.Length);
                                stringBuilder.Append(stringValue);
                                if (includeInResult)
                                {
                                    offsetResultBuilder.Append(stringResultBuilder.Length);
                                }
                                // For dense mode, concatenation doesn't slice the child arrays, so always
                                // add the value to the result.
                                stringResultBuilder.Append(stringValue);
                            }
                            else
                            {
                                offsetBuilder.Append(intBuilder.Length);
                                intBuilder.Append(intValue);
                                if (includeInResult)
                                {
                                    offsetResultBuilder.Append(intResultBuilder.Length);
                                }
                                intResultBuilder.Append(intValue);
                            }
                        }
                        else
                        {
                            stringBuilder.Append(stringValue);
                            intBuilder.Append(intValue);
                            if (includeInResult)
                            {
                                stringResultBuilder.Append(stringValue);
                                intResultBuilder.Append(intValue);
                            }
                        }

                        if (dataList[j] == null)
                        {
                            nullCount++;
                            if (includeInResult)
                            {
                                resultNullCount++;
                            }
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

                    var unionArray = UnionArray.Create(new ArrayData(
                        type, dataList.Count, nullCount, 0, buffers,
                        new[] { stringBuilder.Build().Data, intBuilder.Build().Data }));
                    TestTargetArrayList.Add(SliceTargetArray(unionArray, i));
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
                    type, _resultTotalElementCount, resultNullCount, 0, resultBuffers,
                        new[] { stringResultBuilder.Build().Data, intResultBuilder.Build().Data }));
            }

            public void Visit(MapType type) =>
                GenerateTestData<MapArray, MapArray.Builder>(type, (builder, x) =>
                {
                    var keyBuilder = (StringArray.Builder)builder.KeyBuilder;
                    var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

                    builder.Append();
                    keyBuilder.Append(x.ToString());
                    valueBuilder.Append(x);
                }, initAction: (builder, length) =>
                {
                    builder.Reserve(length);
                    builder.KeyBuilder.Reserve(length);
                    builder.ValueBuilder.Reserve(length);
                });

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }

            private void GenerateTestData<T, TArray, TArrayBuilder>(IArrowType type, Func<int, T> generator)
                where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>
                where TArray : IArrowArray
            {
                GenerateTestData<TArray, TArrayBuilder>(type, (builder, x) => builder.Append(generator(x)));
            }

            private void GenerateTestData<TArray, TArrayBuilder>(
                IArrowType type, Action<TArrayBuilder, int> buildAction, Action<TArrayBuilder, int> initAction=null)
                where TArrayBuilder : IArrowArrayBuilder<TArray, TArrayBuilder>
                where TArray : IArrowArray
            {
                var resultBuilder = (TArrayBuilder)ArrowArrayBuilderFactory.Build(type);
                if (initAction != null)
                {
                    initAction(resultBuilder, _resultTotalElementCount);
                }
                else
                {
                    resultBuilder.Reserve(_resultTotalElementCount);
                }

                for (int i = 0; i < _baseDataListCount; i++)
                {
                    List<int?> dataList = _baseData[i];
                    var builder = (TArrayBuilder)ArrowArrayBuilderFactory.Build(type);
                    if (initAction != null)
                    {
                        initAction(builder, dataList.Count);
                    }
                    else
                    {
                        builder.Reserve(dataList.Count);
                    }

                    for (int j = 0; j < dataList.Count; ++j)
                    {
                        var value = dataList[j];
                        if (value.HasValue)
                        {
                            buildAction(builder, value.Value);
                            if (IncludeInResult(i, j))
                            {
                                buildAction(resultBuilder, value.Value);
                            }
                        }
                        else
                        {
                            builder.AppendNull();
                            if (IncludeInResult(i, j))
                            {
                                resultBuilder.AppendNull();
                            }
                        }
                    }

                    TestTargetArrayList.Add(SliceTargetArray(builder.Build(default), i));
                }

                ExpectedArray = resultBuilder.Build(default);
            }

            private bool IncludeInResult(int listIndex, int itemIndex)
            {
                if (_sliceParameters == null)
                {
                    // Unsliced arrays, all values are expected in the result
                    return true;
                }

                var sliceParameters = _sliceParameters[listIndex];
                return itemIndex >= sliceParameters.Offset &&
                       itemIndex < (sliceParameters.Offset + sliceParameters.Length);
            }

            private IArrowArray SliceTargetArray(IArrowArray array, int targetIndex)
            {
                if (_sliceParameters == null)
                {
                    return array;
                }

                return ArrowArrayFactory.Slice(
                    array, _sliceParameters[targetIndex].Offset, _sliceParameters[targetIndex].Length);
            }
        }
    }
}
