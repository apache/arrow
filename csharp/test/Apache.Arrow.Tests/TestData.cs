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

namespace Apache.Arrow.Tests
{
    public static class TestData
    {
        public static RecordBatch CreateSampleRecordBatch(int length)
        {
            Schema.Builder builder = new Schema.Builder();
            builder.Field(CreateField(BooleanType.Default));
            builder.Field(CreateField(UInt8Type.Default));
            builder.Field(CreateField(Int8Type.Default));
            builder.Field(CreateField(UInt16Type.Default));
            builder.Field(CreateField(Int16Type.Default));
            builder.Field(CreateField(UInt32Type.Default));
            builder.Field(CreateField(Int32Type.Default));
            builder.Field(CreateField(UInt64Type.Default));
            builder.Field(CreateField(Int64Type.Default));
            builder.Field(CreateField(FloatType.Default));
            builder.Field(CreateField(DoubleType.Default));
            //builder.Field(CreateField(new DecimalType(19, 2)));
            //builder.Field(CreateField(HalfFloatType.Default));
            //builder.Field(CreateField(StringType.Default));
            //builder.Field(CreateField(Date32Type.Default));
            //builder.Field(CreateField(Date64Type.Default));
            //builder.Field(CreateField(Time32Type.Default));
            //builder.Field(CreateField(Time64Type.Default));
            //builder.Field(CreateField(TimestampType.Default));

            Schema schema = builder.Build();

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
                case ArrowTypeId.UInt8:
                    ArrowBuffer.Builder<byte> byteBuilder = new ArrowBuffer.Builder<byte>(length);
                    for (int i = 0; i < length; i++)
                        byteBuilder.Append((byte)i);
                    return new UInt8Array(byteBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Int8:
                    ArrowBuffer.Builder<sbyte> sbyteBuilder = new ArrowBuffer.Builder<sbyte>(length);
                    for (int i = 0; i < length; i++)
                        sbyteBuilder.Append((sbyte)i);
                    return new Int8Array(sbyteBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.UInt16:
                    ArrowBuffer.Builder<ushort> ushortBuilder = new ArrowBuffer.Builder<ushort>(length);
                    for (int i = 0; i < length; i++)
                        ushortBuilder.Append((ushort)i);
                    return new UInt16Array(ushortBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Int16:
                    ArrowBuffer.Builder<short> shortBuilder = new ArrowBuffer.Builder<short>(length);
                    for (int i = 0; i < length; i++)
                        shortBuilder.Append((short)i);
                    return new Int16Array(shortBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.UInt32:
                    ArrowBuffer.Builder<uint> uintBuilder = new ArrowBuffer.Builder<uint>(length);
                    for (int i = 0; i < length; i++)
                        uintBuilder.Append((uint)i);
                    return new UInt32Array(uintBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Int32:
                    ArrowBuffer.Builder<int> intBuilder = new ArrowBuffer.Builder<int>(length);
                    for (int i = 0; i < length; i++)
                        intBuilder.Append(i);
                    return new Int32Array(intBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.UInt64:
                    ArrowBuffer.Builder<ulong> ulongBuilder = new ArrowBuffer.Builder<ulong>(length);
                    for (int i = 0; i < length; i++)
                        ulongBuilder.Append((ulong)i);
                    return new UInt64Array(ulongBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Int64:
                    ArrowBuffer.Builder<long> longBuilder = new ArrowBuffer.Builder<long>(length);
                    for (int i = 0; i < length; i++)
                        longBuilder.Append(i);
                    return new Int64Array(longBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Float:
                    ArrowBuffer.Builder<float> floatBuilder = new ArrowBuffer.Builder<float>(length);
                    for (int i = 0; i < length; i++)
                        floatBuilder.Append(i);
                    return new FloatArray(floatBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                case ArrowTypeId.Double:
                    ArrowBuffer.Builder<double> doubleBuilder = new ArrowBuffer.Builder<double>(length);
                    for (int i = 0; i < length; i++)
                        doubleBuilder.Append(i);
                    return new DoubleArray(doubleBuilder.Build(), ArrowBuffer.Empty, length, 0, 0);
                //TODO: there is no DecimalArray
                //case ArrowTypeId.Decimal:
                //    ArrowBuffer.Builder<decimal> builder = new ArrowBuffer.Builder<decimal>(length);
                //    for (int i = 0; i < length; i++)
                //        builder.Append(i);
                //    return new DecimalArray
                //    break;

                //case ArrowTypeId.HalfFloat:
                //    break;
                //case ArrowTypeId.String:
                //    break;
                //case ArrowTypeId.Date32:
                //    break;
                //case ArrowTypeId.Date64:
                //    break;
                //case ArrowTypeId.Time32:
                //    break;
                //case ArrowTypeId.Time64:
                //    break;
                //case ArrowTypeId.Timestamp:
                //    break;

                default:
                    throw new NotSupportedException($"Could not create an array for type '{field.DataType.TypeId}'");
            }
        }
    }
}
