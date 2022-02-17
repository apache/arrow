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
using System.Reflection;
using Apache.Arrow.Memory;
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
                IArrowArray actualArray = ArrowArrayConcatenatorReflector.InvokeConcatenate(testTargetArrayList);
                ArrowReaderVerifier.CompareArrays(expectedArray, actualArray);
            }
        }

        [Fact]
        public void TestNullOrEmpty()
        {
            Assert.Null(ArrowArrayConcatenatorReflector.InvokeConcatenate(null));
            Assert.Null(ArrowArrayConcatenatorReflector.InvokeConcatenate(new List<IArrowArray>()));
        }

        [Fact]
        public void TestSingleElement()
        {
            Int32Array array = new Int32Array.Builder().Append(1).Append(2).Build();
            IArrowArray actualArray = ArrowArrayConcatenatorReflector.InvokeConcatenate(new[] { array });
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
                    StringType.Default,
                    Date32Type.Default,
                    Date64Type.Default,
                    TimestampType.Default,
                    new Decimal128Type(14, 10),
                    new Decimal256Type(14,10),
                    new ListType(Int64Type.Default),
                    new StructType(new List<Field>{
                        new Field.Builder().Name("Strings").DataType(StringType.Default).Nullable(true).Build(),
                        new Field.Builder().Name("Ints").DataType(Int32Type.Default).Nullable(true).Build()
                    }),
                };

            foreach (IArrowType type in targetTypes)
            {
                var creator = new TestDataGenerator();
                type.Accept(creator);
                yield return Tuple.Create(creator.TestTargetArrayList, creator.ExpectedArray);
            }
        }

        private static class ArrowArrayConcatenatorReflector
        {
            private static readonly MethodInfo s_concatenateInfo = typeof(ArrayData).Assembly.GetType("Apache.Arrow.ArrowArrayConcatenator")
                .GetMethod("Concatenate", BindingFlags.Static | BindingFlags.NonPublic);

            internal static IArrowArray InvokeConcatenate(IReadOnlyList<IArrowArray> arrowArrayList, MemoryAllocator allocator = default)
            {
                return s_concatenateInfo.Invoke(null, new object[] { arrowArrayList, allocator }) as IArrowArray;
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
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<StructType>
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

            public void Visit(ListType type)
            {
                ListArray.Builder resultBuilder = new ListArray.Builder(type.ValueDataType).Reserve(_baseDataTotalElementCount);
                //Todo : Support various types
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

                ExpectedArray = new StructArray(type, 3, new List<Array> { resultStringArray, resultInt32Array }, nullBitmapBuffer, 1);
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
