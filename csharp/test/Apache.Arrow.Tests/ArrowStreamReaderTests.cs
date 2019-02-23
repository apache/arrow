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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamReaderTests
    {
        [Fact]
        public async Task ReadRecordBatch()
        {
            RecordBatch originalBatch = CreateSampleRecordBatch();

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                buffer = stream.GetBuffer();
            }

            ArrowStreamReader reader = new ArrowStreamReader(buffer);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            CompareBatches(originalBatch, readBatch);

            // There should only be one batch - calling ReadNextRecordBatch again should return null.
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Null(reader.ReadNextRecordBatch());
        }

        private void CompareBatches(RecordBatch expectedBatch, RecordBatch actualBatch)
        {
            CompareSchemas(expectedBatch.Schema, actualBatch.Schema);
            Assert.Equal(expectedBatch.Length, actualBatch.Length);
            Assert.Equal(expectedBatch.ColumnCount, actualBatch.ColumnCount);

            for (int i = 0; i < expectedBatch.ColumnCount; i++)
            {
                IArrowArray expectedArray = expectedBatch.Arrays.ElementAt(i);
                IArrowArray actualArray = actualBatch.Arrays.ElementAt(i);

                CompareArrays(expectedArray, actualArray);
            }
        }

        private void CompareSchemas(Schema expectedSchema, Schema actualSchema)
        {
            Assert.Equal(expectedSchema.Fields.Count, actualSchema.Fields.Count);
            // TODO: compare fields
        }

        private void CompareArrays(IArrowArray expectedArray, IArrowArray actualArray)
        {
            Assert.Equal(expectedArray.Length, actualArray.Length);
            Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
            Assert.Equal(expectedArray.Offset, actualArray.Offset);
            CompareArrayData(expectedArray.Data, actualArray.Data);
        }

        private void CompareArrayData(ArrayData expectedData, ArrayData actualData)
        {
            Assert.Equal(expectedData.Length, actualData.Length);
            Assert.Equal(expectedData.Buffers.Length, actualData.Buffers.Length);

            for (int i = 0; i < expectedData.Buffers.Length; i++)
            {
                CompareBuffers(expectedData.Buffers[i], actualData.Buffers[i]);
            }
        }

        private void CompareBuffers(ArrowBuffer expectedBuffer, ArrowBuffer actualBuffer)
        {
            Assert.Equal(expectedBuffer.Length, actualBuffer.Length);
            Assert.True(expectedBuffer.Span.SequenceEqual(actualBuffer.Span));
        }

        private static RecordBatch CreateSampleRecordBatch()
        {
            Schema.Builder builder = new Schema.Builder();
            builder.Field(CreateField(BooleanType.Default));
            //builder.Field(CreateField(Date32Type.Default));
            //builder.Field(CreateField(Date64Type.Default));
            //builder.Field(CreateField(new DecimalType(19, 2)));
            //builder.Field(CreateField(DoubleType.Default));
            //builder.Field(CreateField(FloatType.Default));
            //builder.Field(CreateField(HalfFloatType.Default));
            //builder.Field(CreateField(Int16Type.Default));
            //builder.Field(CreateField(Int32Type.Default));
            builder.Field(CreateField(Int64Type.Default));
            //builder.Field(CreateField(Int8Type.Default));
            //builder.Field(CreateField(StringType.Default));
            //builder.Field(CreateField(Time32Type.Default));
            //builder.Field(CreateField(Time64Type.Default));
            //builder.Field(CreateField(TimestampType.Default));
            //builder.Field(CreateField(UInt16Type.Default));
            //builder.Field(CreateField(UInt32Type.Default));
            //builder.Field(CreateField(UInt64Type.Default));
            //builder.Field(CreateField(UInt8Type.Default));

            Schema schema = builder.Build();

            const int length = 100;
            IEnumerable<IArrowArray> arrays = CreateArrays(schema, length);

            return new RecordBatch(builder.Build(), arrays, length);
        }

        private static Field CreateField(ArrowType type)
        {
            return new Field(type.Name, type, nullable: false);
        }

        private static IEnumerable<IArrowArray> CreateArrays(Schema schema, int length)
        {
            int fieldCount = schema.Fields.Count;
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
            switch (field.DataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    ArrowBuffer.Builder<bool> boolBuilder = new ArrowBuffer.Builder<bool>(length);
                    for (int i = 0; i < length; i++)
                        boolBuilder.Append(i % 2 == 0);
                    return new BooleanArray(boolBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);

                //case ArrowTypeId.UInt8:
                //    break;
                //case ArrowTypeId.Int8:
                //    break;
                //case ArrowTypeId.UInt16:
                //    break;
                //case ArrowTypeId.Int16:
                //    break;
                //case ArrowTypeId.UInt32:
                //    break;
                //case ArrowTypeId.Int32:
                //    break;
                //case ArrowTypeId.UInt64:
                //    break;
                case ArrowTypeId.Int64:
                    ArrowBuffer.Builder<long> longBuilder = new ArrowBuffer.Builder<long>(length);
                    for (int i = 0; i < length; i++)
                        longBuilder.Append(i);
                    return new Int64Array(longBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                //case ArrowTypeId.HalfFloat:
                //    break;
                //case ArrowTypeId.Float:
                //    break;
                //case ArrowTypeId.Double:
                //    break;
                //case ArrowTypeId.String:
                //    break;
                //case ArrowTypeId.Date32:
                //    break;
                //case ArrowTypeId.Date64:
                //    break;
                //case ArrowTypeId.Timestamp:
                //    break;
                //case ArrowTypeId.Time32:
                //    break;
                //case ArrowTypeId.Time64:
                //    break;

                //TODO: there is no DecimalArray
                //case ArrowTypeId.Decimal:
                //    ArrowBuffer.Builder<decimal> builder = new ArrowBuffer.Builder<decimal>(length);
                //    for (int i = 0; i < length; i++)
                //        builder.Append(i);
                //    return new DecimalArray
                //    break;

                default:
                    throw new NotSupportedException($"Could not create an array for type '{field.DataType.TypeId}'");
            }
        }
    }
}
