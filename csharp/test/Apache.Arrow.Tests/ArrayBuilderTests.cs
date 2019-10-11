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
using System.Reflection.Metadata;
using Apache.Arrow.Arrays;
using Apache.Arrow.Arrays.DictionaryArrays;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayBuilderTests
    {
        // TODO: Test various builder invariants (Append, AppendRange, Clear, Resize, Reserve, etc)

        private static readonly string[] StringDictionaryElems = new string[] {"string1", "string2", "string3" };
        private static readonly string[] StringDictionaryElemsDupes = new string[] {"string1", "string2", "string3"};

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
        public void PrimitiveDictionaryArrayBuildersProduceExpectedArray()
        {
            // simple case, no duplicates
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<sbyte, Int8DictionaryArray, Int8DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<short, Int16DictionaryArray, Int16DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<int, Int32DictionaryArray, Int32DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<long, Int64DictionaryArray, Int64DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<byte, UInt8DictionaryArray, UInt8DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<ushort, UInt16DictionaryArray, UInt16DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<uint, UInt32DictionaryArray, UInt32DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestPrimitiveDictionaryArrayBuilderNoDuplicates<ulong, UInt64DictionaryArray, UInt64DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(30));

            // with a duplicate value
            TestPrimitiveDictionaryArrayBuilderDuplicates<sbyte, Int8DictionaryArray, Int8DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<short, Int16DictionaryArray, Int16DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<int, Int32DictionaryArray, Int32DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<long, Int64DictionaryArray, Int64DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<byte, UInt8DictionaryArray, UInt8DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<ushort, UInt16DictionaryArray, UInt16DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<uint, UInt32DictionaryArray, UInt32DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
            TestPrimitiveDictionaryArrayBuilderDuplicates<ulong, UInt64DictionaryArray, UInt64DictionaryArray.Builder>(x => x.Append(10).Append(20).Append(20));
        }

        [Fact]
        public void TestStringDictionaryArrayBuilderSimpleCase()
        {
            var dictArr = new StringDictionaryArray.StringDictionaryBuilder()
                .AppendRange(StringDictionaryElems)
                .Build(default);

            Assert.Equal(3, dictArr.Length);
            Assert.Equal(3, dictArr.Indices.Length);
            Assert.Equal(0, dictArr.NullCount);

            var arr = new StringArray.Builder()
                .AppendRange(StringDictionaryElems)
                .Build(default);


            for (int i = 0; i < arr.Length; i++)
            {
                Assert.Equal(dictArr.GetString(i), arr.GetString(i));
            }
        }

        [Fact]
        public void TestStringDictionaryArrayBuilderDuplicates()
        {
            var dictArr = new StringDictionaryArray.StringDictionaryBuilder()
                .AppendRange(StringDictionaryElems)
                .AppendRange(StringDictionaryElems)
                .Build(default);

            Assert.Equal(6, dictArr.Length);
            Assert.Equal(6, dictArr.Indices.Length);
            Assert.Equal(0, dictArr.NullCount);
            Assert.Equal(3, dictArr.UniqueValuesCount);


            var arr = new StringArray.Builder()
                .AppendRange(StringDictionaryElems)
                .AppendRange(StringDictionaryElems)
                .Build(default);


            for (int i = 0; i < arr.Length; i++)
            {
                Assert.Equal(dictArr.GetString(i), arr.GetString(i));
            }
        }

        [Fact]
        public void TestStringDictionaryArrayBuilderDuplicatesAndNulls()
        {
            var dictArr = new StringDictionaryArray.StringDictionaryBuilder()
                .Append((string)null)
                .AppendRange(StringDictionaryElems)
                .AppendRange(StringDictionaryElems)
                .Build(default);

            Assert.Equal(7, dictArr.Length);
            Assert.Equal(7, dictArr.Indices.Length);
            Assert.Equal(1, dictArr.NullCount);
            Assert.Equal(3, dictArr.UniqueValuesCount);

            var arr = new StringArray.Builder()
                .AppendRange(StringDictionaryElems)
                .AppendRange(StringDictionaryElems)
                .Build(default);


            for (int i = 0; i < arr.Length; i++)
            {
                Assert.Equal(dictArr.GetString(i+1), arr.GetString(i));
            }
        }

        public class TimestampArrayBuilder
        {
            [Fact]
            public void ProducesExpectedArray()
            {
                var now = DateTimeOffset.UtcNow.ToLocalTime();
                var array = new TimestampArray.Builder(TimeUnit.Nanosecond, TimeZoneInfo.Local.Id)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                Assert.NotNull(array.GetTimestamp(0));
                Assert.Equal(now.Truncate(TimeSpan.FromTicks(100)), array.GetTimestamp(0).Value);
            }
        }

        private static void TestArrayBuilder<TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where TArray: IArrowArray
            where TArrayBuilder: IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var array = builder.Build(default);

            Assert.IsAssignableFrom<TArray>(array);
            Assert.NotNull(array);
            Assert.Equal(3, array.Length);
            Assert.Equal(0, array.NullCount);
        }

        private static void TestPrimitiveDictionaryArrayBuilderNoDuplicates<T, TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where T : struct, IEquatable<T>
            where TArray : PrimitiveDictionaryArray<T>, IDictionaryArray
            where TArrayBuilder : IDictionaryArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var array = builder.Build(default);
            Assert.NotNull(array);
            Assert.IsAssignableFrom<TArray>(array);
            Assert.Equal(3, array.Length);
            Assert.Equal(3, array.Indices.Length);
            Assert.Equal(0, array.NullCount);
            Assert.Equal(3, array.Values.Length);

        }

        private static void TestPrimitiveDictionaryArrayBuilderDuplicates<T, TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where T : struct, IEquatable<T>
            where TArray : PrimitiveDictionaryArray<T>, IDictionaryArray
            where TArrayBuilder : IDictionaryArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();

            action(builder);
            var array = (PrimitiveDictionaryArray<T>)builder.Build(default);
            Assert.NotNull(array);
            Assert.IsAssignableFrom<PrimitiveDictionaryArray<T>>(array);
            Assert.Equal(3, array.Length);
            Assert.Equal(3, array.Indices.Length);
            Assert.Equal(2, array.Values.Length);
            Assert.Equal(0, array.NullCount);
        }

    }
}
