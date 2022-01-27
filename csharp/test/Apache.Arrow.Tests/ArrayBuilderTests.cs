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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayBuilderTests
    {
        // TODO: Test various builder invariants (Append, AppendRange, Clear, Resize, Reserve, etc)

        [Fact]
        public void PrimitiveArrayBuildersProduceExpectedArray()
        {
            TestArrayBuilder<Int8Array, Int8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int16Array, Int16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int32Array, Int32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int64Array, Int64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt8Array, UInt8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt16Array, UInt16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt32Array, UInt32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt64Array, UInt64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<FloatArray, FloatArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<DoubleArray, DoubleArray.Builder>(x => x.Append(10).Append(20).Append(30));
        }

        [Fact]
        public void PrimitiveArrayBuildersProduceExpectedArrayWithNulls()
        {
            TestArrayBuilder<Int8Array, Int8Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(127), 4, 2, 0x09);
            TestArrayBuilder<Int16Array, Int16Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<Int32Array, Int32Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<Int64Array, Int64Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<UInt8Array, UInt8Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(127), 4, 2, 0x09);
            TestArrayBuilder<UInt16Array, UInt16Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<UInt32Array, UInt32Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<UInt64Array, UInt64Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<UInt64Array, UInt64Array.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<FloatArray, FloatArray.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
            TestArrayBuilder<DoubleArray, DoubleArray.Builder>(x => x.Append(123).AppendNull().AppendNull().Append(456), 4, 2, 0x09);
        }

        [Fact]
        public void BooleanArrayBuilderProducersExpectedArray()
        {
            TestArrayBuilder<BooleanArray, BooleanArray.Builder>(x => x.Append(true).Append(false).Append(true));
            TestArrayBuilder<BooleanArray, BooleanArray.Builder>(x => x.Append(true).AppendNull().Append(false).Append(true), 4, 1, 0x0D);
        }

        [Fact]
        public void StringArrayBuilderHandlesNullsAndEmptyStrings()
        {
            var stringArray = TestArrayBuilder<StringArray, StringArray.Builder>(x => x.Append("123").Append(null).AppendNull().Append(string.Empty), 4, 2, 0x09);
            Assert.Equal("123", stringArray.GetString(0));
            Assert.Null(stringArray.GetString(1));
            Assert.Null(stringArray.GetString(2));
            Assert.Equal(string.Empty, stringArray.GetString(3));
        }


        [Fact]
        public void ListArrayBuilder()
        {
            var listBuilder = new ListArray.Builder(StringType.Default);
            var valueBuilder = listBuilder.ValueBuilder as StringArray.Builder;
            Assert.NotNull(valueBuilder);
            listBuilder.Append();
            valueBuilder.Append("1");
            listBuilder.AppendNull();
            listBuilder.Append();
            valueBuilder.Append("22").Append("33");
            listBuilder.Append();
            valueBuilder.Append("444").AppendNull().Append("555").Append("666");

            var list = listBuilder.Build();

            Assert.Equal(
                new List<string> { "1" },
                ConvertStringArrayToList(list.GetSlicedValues(0) as StringArray));
            Assert.Null(list.GetSlicedValues(1));
            Assert.Equal(
                new List<string> { "22", "33" },
                ConvertStringArrayToList(list.GetSlicedValues(2) as StringArray));
            Assert.Equal(
                new List<string> { "444", null, "555", "666" },
                ConvertStringArrayToList(list.GetSlicedValues(3) as StringArray));

            Assert.Throws<ArgumentOutOfRangeException>(() => list.GetValueLength(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.GetValueLength(4));

            listBuilder.Resize(2);
            var truncatedList = listBuilder.Build();

            Assert.Equal(
                new List<string> { "22", "33", "444", null, "555", "666" },
                ConvertStringArrayToList(truncatedList.GetSlicedValues(2) as StringArray));

            Assert.Throws<ArgumentOutOfRangeException>(() => truncatedList.GetSlicedValues(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => truncatedList.GetSlicedValues(3));

            listBuilder.Clear();
            var emptyList = listBuilder.Build();

            Assert.Equal(0, emptyList.Length);

            List<string> ConvertStringArrayToList(StringArray array)
            {
                var length = array.Length;
                var resultList = new List<string>(length);
                for (var index = 0; index < length; index++)
                {
                    resultList.Add(array.GetString(index));
                }
                return resultList;
            }
        }

        [Fact]
        public void ListArrayBuilderValidityBuffer()
        {
            ListArray listArray = new ListArray.Builder(Int64Type.Default).Append().AppendNull().Build();   
            Assert.False(listArray.IsValid(2));
        }

        [Fact]
        public void NestedListArrayBuilder()
        {
            var childListType = new ListType(Int64Type.Default);
            var parentListBuilder = new ListArray.Builder(childListType);
            var childListBuilder = parentListBuilder.ValueBuilder as ListArray.Builder;
            Assert.NotNull(childListBuilder);
            var valueBuilder = childListBuilder.ValueBuilder as Int64Array.Builder;
            Assert.NotNull(valueBuilder);

            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(1);
            childListBuilder.Append();
            valueBuilder.Append(2).Append(3);
            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(4).Append(5).Append(6).Append(7);
            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(8).Append(9).Append(10).Append(11).Append(12);

            var parentList = parentListBuilder.Build();

            var childList1 = (ListArray)parentList.GetSlicedValues(0);
            var childList2 = (ListArray)parentList.GetSlicedValues(1);
            var childList3 = (ListArray)parentList.GetSlicedValues(2);

            Assert.Equal(2, childList1.Length);
            Assert.Equal(1, childList2.Length);
            Assert.Equal(1, childList3.Length);
            Assert.Equal(
                new List<long?> { 1 },
                ((Int64Array)childList1.GetSlicedValues(0)).ToList());
            Assert.Equal(
                new List<long?> { 2, 3 },
                ((Int64Array)childList1.GetSlicedValues(1)).ToList());
            Assert.Equal(
                new List<long?> { 4, 5, 6, 7 },
                ((Int64Array)childList2.GetSlicedValues(0)).ToList());
            Assert.Equal(
                new List<long?> { 8, 9, 10, 11, 12 },
                ((Int64Array)childList3.GetSlicedValues(0)).ToList());
        }

        public class TimestampArrayBuilder
        {
            [Fact]
            public void ProducesExpectedArray()
            {
                var now = DateTimeOffset.UtcNow.ToLocalTime();
                var timestampType = new TimestampType(TimeUnit.Nanosecond, TimeZoneInfo.Local);
                var array = new TimestampArray.Builder(timestampType)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                var value = array.GetTimestamp(0);
                Assert.NotNull(value);
                Assert.Equal(now, value.Value);

                timestampType = new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Local);
                array = new TimestampArray.Builder(timestampType)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                value = array.GetTimestamp(0);
                Assert.NotNull(value);
                Assert.Equal(now.Truncate(TimeSpan.FromTicks(10)), value.Value);

                timestampType = new TimestampType(TimeUnit.Millisecond, TimeZoneInfo.Local);
                array = new TimestampArray.Builder(timestampType)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                value = array.GetTimestamp(0);
                Assert.NotNull(value);
                Assert.Equal(now.Truncate(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond)), value.Value);
            }
        }

        private static TArray TestArrayBuilder<TArray, TArrayBuilder>(Action<TArrayBuilder> action, int expectedLength = 3, int expectedNullCount = 0, int expectedNulls = 0)
            where TArray : IArrowArray
            where TArrayBuilder : IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var array = builder.Build(default);

            Assert.IsAssignableFrom<TArray>(array);
            Assert.NotNull(array);
            Assert.Equal(expectedLength, array.Length);
            Assert.Equal(expectedNullCount, array.NullCount);
            if (expectedNulls != 0)
            {
                Assert.True(array.Data.Buffers[0].Span.Slice(0, 1).SequenceEqual(new ReadOnlySpan<byte>(BitConverter.GetBytes(expectedNulls).Take(1).ToArray())));
            }
            return array;
        }

    }
}
