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

            return new RecordBatch(schema, arrays, length);
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
            var creator = new ArrayBufferCreator(length);
            field.DataType.Accept(creator);

            ArrayData data = new ArrayData(field.DataType, length, 0, 0,
                    new[] { ArrowBuffer.Empty, creator.Buffer });

            return ArrowArrayFactory.BuildArray(data);
        }

        private class ArrayBufferCreator :
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
            IArrowTypeVisitor<DoubleType>
        {
            private readonly int _length;
            public ArrowBuffer Buffer { get; private set; }

            public ArrayBufferCreator(int length)
            {
                _length = length;
            }

            public void Visit(BooleanType type)
            {
                ArrowBuffer.Builder<bool> builder = new ArrowBuffer.Builder<bool>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append(i % 2 == 0);

                Buffer = builder.Build();
            }

            public void Visit(Int8Type type)
            {
                ArrowBuffer.Builder<sbyte> builder = new ArrowBuffer.Builder<sbyte>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append((sbyte)i);

                Buffer = builder.Build();
            }

            public void Visit(UInt8Type type)
            {
                ArrowBuffer.Builder<byte> builder = new ArrowBuffer.Builder<byte>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append((byte)i);

                Buffer = builder.Build();
            }

            public void Visit(Int16Type type)
            {
                ArrowBuffer.Builder<short> builder = new ArrowBuffer.Builder<short>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append((short)i);

                Buffer = builder.Build();
            }

            public void Visit(UInt16Type type)
            {
                ArrowBuffer.Builder<ushort> builder = new ArrowBuffer.Builder<ushort>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append((ushort)i);

                Buffer = builder.Build();
            }

            public void Visit(Int32Type type) => CreateNumberArray<int>(type);
            public void Visit(UInt32Type type) => CreateNumberArray<uint>(type);
            public void Visit(Int64Type type) => CreateNumberArray<long>(type);
            public void Visit(UInt64Type type) => CreateNumberArray<ulong>(type);
            public void Visit(FloatType type) => CreateNumberArray<float>(type);
            public void Visit(DoubleType type) => CreateNumberArray<double>(type);

            private void CreateNumberArray<T>(IArrowType type)
                where T : struct
            {
                ArrowBuffer.Builder<T> builder = new ArrowBuffer.Builder<T>(_length);
                for (int i = 0; i < _length; i++)
                    builder.Append((T)Convert.ChangeType(i, typeof(T)));

                Buffer = builder.Build();
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }
        }
    }
}
