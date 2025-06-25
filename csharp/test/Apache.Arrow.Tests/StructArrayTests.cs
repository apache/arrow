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

using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class StructArrayTests
    {
        [Fact]
        public void TestStructArray()
        {
            // The following can be improved with a Builder class for StructArray.
            List<Field> fields = new List<Field>();
            Field.Builder fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Strings").DataType(StringType.Default).Nullable(true).Build());
            fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Ints").DataType(Int32Type.Default).Nullable(true).Build());
            StructType structType = new StructType(fields);

            StringArray.Builder stringBuilder = new StringArray.Builder();
            StringArray stringArray = stringBuilder.Append("joe").AppendNull().AppendNull().Append("mark").Build();
            Int32Array.Builder intBuilder = new Int32Array.Builder();
            Int32Array intArray = intBuilder.Append(1).Append(2).AppendNull().Append(4).Build();
            List<Array> arrays = new List<Array>();
            arrays.Add(stringArray);
            arrays.Add(intArray);

            ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
            var nullBitmapBuffer = nullBitmap.Append(true).Append(true).Append(false).Append(true).Build();
            StructArray structs = new StructArray(structType, 4, arrays, nullBitmapBuffer, 1);

            Assert.Equal(4, structs.Length);
            Assert.Equal(1, structs.NullCount);
            ArrayData[] childArrays = structs.Data.Children; // Data for StringArray and Int32Array
            Assert.Equal(2, childArrays.Length);
            for (int i = 0; i < childArrays.Length; i++)
            {
                ArrayData arrayData = childArrays[i];
                Assert.Null(arrayData.Children);
                if (i == 0)
                {
                    Assert.Equal(ArrowTypeId.String, arrayData.DataType.TypeId);
                    Array array = new StringArray(arrayData);
                    StringArray structStringArray = array as StringArray;
                    Assert.NotNull(structStringArray);
                    Assert.Equal(structs.Length, structStringArray.Length);
                    Assert.Equal(stringArray.Length, structStringArray.Length);
                    Assert.Equal(stringArray.NullCount, structStringArray.NullCount);
                    for (int j = 0; j < stringArray.Length; j++)
                    {
                        Assert.Equal(stringArray.GetString(j), structStringArray.GetString(j));
                    }
                }
                if (i == 1)
                {
                    Assert.Equal(ArrowTypeId.Int32, arrayData.DataType.TypeId);
                    Array array = new Int32Array(arrayData);
                    Int32Array structIntArray = array as Int32Array;
                    Assert.NotNull(structIntArray);
                    Assert.Equal(structs.Length, structIntArray.Length);
                    Assert.Equal(intArray.Length, structIntArray.Length);
                    Assert.Equal(intArray.NullCount, structIntArray.NullCount);
                    for (int j = 0; j < intArray.Length; j++)
                    {
                        Assert.Equal(intArray.GetValue(j), structIntArray.GetValue(j));
                    }
                }
            }
        }

        [Fact]
        public void TestListOfStructArray()
        {
            Schema.Builder builder = new Schema.Builder();
            Field structField = new Field(
                "struct",
                new StructType(
                    new[]
                    {
                        new Field("name", StringType.Default, nullable: false),
                        new Field("age", Int64Type.Default, nullable: false),
                    }),
                nullable: false);

            Field listField = new Field("listOfStructs", new ListType(structField), nullable: false);
            builder.Field(listField);
            Schema schema = builder.Build();

            StringArray stringArray = new StringArray.Builder()
                .Append("joe").AppendNull().AppendNull().Append("mark").Append("abe").Append("phil").Build();
            Int64Array intArray = new Int64Array.Builder()
                .Append(1).Append(2).AppendNull().Append(4).Append(10).Append(55).Build();

            ArrowBuffer nullBitmapBuffer = new ArrowBuffer.BitmapBuilder()
                .Append(true).Append(true).Append(false).Append(true).Append(true).Append(true).Build();

            StructArray structs = new StructArray(structField.DataType, 6, new IArrowArray[] { stringArray, intArray }, nullBitmapBuffer, nullCount: 1);

            ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>()
                .Append(0).Append(2).Append(5).Append(6).Build();
            ListArray listArray = new ListArray(listField.DataType, 3, offsetsBuffer, structs, ArrowBuffer.Empty);

            RecordBatch batch = new RecordBatch(schema, new[] { listArray }, 3);
            TestRoundTripRecordBatch(batch);
        }

        [Fact]
        public void TestSliceStructArray()
        {
            const int numRows = 10;
            var fields = new List<Field>
            {
                new Field.Builder().Name("ints").DataType(new Int32Type()).Nullable(true).Build(),
                new Field.Builder().Name("doubles").DataType(new DoubleType()).Nullable(true).Build(),
            };
            var arrays = new List<IArrowArray>
            {
                new Int32Array.Builder().AppendRange(Enumerable.Range(0, numRows)).Build(),
                new DoubleArray.Builder().AppendRange(Enumerable.Range(0, numRows).Select(i => i * 0.1)).Build(),
            };

            var nullBitmap = new ArrowBuffer.BitmapBuilder().AppendRange(true, numRows).Build();
            var array = new StructArray(new StructType(fields), numRows, arrays, nullBitmap, nullCount: 0);

            var slicedArray = (StructArray) array.Slice(3, 4);

            Assert.Equal(4, slicedArray.Length);
            Assert.Equal(2, slicedArray.Fields.Count);

            var slicedInts = slicedArray.Fields[0];
            var expectedInts = Enumerable.Range(3, 4).Select(val => (int?) val).ToArray();
            Assert.Equal(expectedInts, (IReadOnlyList<int?>) slicedInts);

            var slicedDoubles = slicedArray.Fields[1];
            var expectedDoubles = Enumerable.Range(3, 4).Select(val => (double?) (val * 0.1)).ToArray();
            Assert.Equal(expectedDoubles, (IReadOnlyList<double?>) slicedDoubles);
        }

        [Fact]
        public void TestStructArrayConstructedWithOffset()
        {
            const int dataNumRows = 10;
            const int arrayLength = 4;
            const int arrayOffset = 3;

            var fields = new List<Field>
            {
                new Field.Builder().Name("ints").DataType(new Int32Type()).Nullable(true).Build(),
                new Field.Builder().Name("doubles").DataType(new DoubleType()).Nullable(true).Build(),
            };
            var arrays = new List<IArrowArray>
            {
                new Int32Array.Builder().AppendRange(Enumerable.Range(0, dataNumRows)).Build(),
                new DoubleArray.Builder().AppendRange(Enumerable.Range(0, dataNumRows).Select(i => i * 0.1)).Build(),
            };

            var nullBitmap = new ArrowBuffer.BitmapBuilder().AppendRange(true, dataNumRows).Build();
            var array = new StructArray(
                new StructType(fields), arrayLength, arrays, nullBitmap, nullCount: 0, offset: arrayOffset);

            Assert.Equal(4, array.Length);
            Assert.Equal(3, array.Offset);
            Assert.Equal(2, array.Fields.Count);

            var slicedInts = array.Fields[0];
            var expectedInts = Enumerable.Range(3, 4).Select(val => (int?) val).ToArray();
            Assert.Equal(expectedInts, (IReadOnlyList<int?>) slicedInts);

            var slicedDoubles = array.Fields[1];
            var expectedDoubles = Enumerable.Range(3, 4).Select(val => (double?) (val * 0.1)).ToArray();
            Assert.Equal(expectedDoubles, (IReadOnlyList<double?>) slicedDoubles);

            var subSlice = (StructArray) array.Slice(1, 2);
            Assert.Equal(2, subSlice.Length);
            Assert.Equal(2, subSlice.Fields.Count);

            var subSlicedInts = subSlice.Fields[0];
            var expectedSubSliceInts = Enumerable.Range(4, 2).Select(val => (int?) val).ToArray();
            Assert.Equal(expectedSubSliceInts, (IReadOnlyList<int?>) subSlicedInts);

            var subSlicedDoubles = subSlice.Fields[1];
            var expectedSubSliceDoubles = Enumerable.Range(4, 2).Select(val => (double?) (val * 0.1)).ToArray();
            Assert.Equal(expectedSubSliceDoubles, (IReadOnlyList<double?>) subSlicedDoubles);
        }

        private static void TestRoundTripRecordBatch(RecordBatch originalBatch)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true))
                {
                    writer.WriteRecordBatch(originalBatch);
                    writer.WriteEnd();
                }

                stream.Position = 0;

                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }
    }
}
