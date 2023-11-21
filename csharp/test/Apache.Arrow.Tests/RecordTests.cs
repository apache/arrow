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

using System.Text;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class RecordTests
    {
        [Fact]
        public void StructArraysAndRecordBatchesAreSimilar()
        {
            Field stringField = new Field("column1", StringType.Default, true);
            StringArray.Builder stringBuilder = new StringArray.Builder();
            StringArray stringArray = stringBuilder.Append("joe").AppendNull().AppendNull().Append("mark").Build();

            Field intField = new Field("column2", Int32Type.Default, true);
            Int32Array.Builder intBuilder = new Int32Array.Builder();
            Int32Array intArray = intBuilder.Append(1).Append(2).AppendNull().Append(4).Build();

            Schema schema = new Schema(new[] { stringField, intField }, null);
            RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { stringArray, intArray }, intArray.Length);
            IArrowRecord structArray1 = batch;

            StructType structType = new StructType(new[] { stringField, intField });
            StructArray structArray = new StructArray(structType, intArray.Length, new IArrowArray[] { stringArray, intArray }, ArrowBuffer.Empty);
            IArrowRecord structArray2 = structArray;

            FieldComparer.Compare(structArray1.Schema, structArray2.Schema);
            Assert.Equal(structArray1.Length, structArray2.Length);
            Assert.Equal(structArray1.ColumnCount, structArray2.ColumnCount);
            Assert.Equal(structArray1.NullCount, structArray2.NullCount);

            for (int i = 0; i < structArray1.ColumnCount; i++)
            {
                ArrowReaderVerifier.CompareArrays(structArray1.Column(i), structArray2.Column(i));
            }
        }

        [Fact]
        public void VisitStructAndBatch()
        {
            Field stringField = new Field("column1", StringType.Default, true);
            StructType level1 = new StructType(new[] { stringField });
            Field level1Field = new Field("column2", level1, false);
            StructType level2 = new StructType(new[] { level1Field });
            Field level2Field = new Field("column3", level2, true);
            Schema schema = new Schema(new[] { level2Field }, null);

            var visitor1 = new TestTypeVisitor1();
            visitor1.Visit(schema);
            Assert.Equal("111utf8", visitor1.stringBuilder.ToString());
            var visitor2 = new TestTypeVisitor2();
            visitor2.Visit(schema);
            Assert.Equal("322utf8", visitor2.stringBuilder.ToString());

            StringArray stringArray = new StringArray.Builder().Append("one").AppendNull().AppendNull().Append("four").Build();
            StructArray level1Array = new StructArray(level1, stringArray.Length, new[] { stringArray }, ArrowBuffer.Empty);
            ArrowBuffer nulls = new ArrowBuffer.BitmapBuilder(stringArray.Length).Append(false).Append(false).Append(true).Append(false).Build();
            StructArray level2Array = new StructArray(level2, stringArray.Length, new[] { level1Array }, nulls);
            RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { level2Array }, stringArray.Length);

            var visitor3 = new TestArrayVisitor1();
            visitor3.Visit(batch);
            Assert.Equal("111utf8", visitor3.stringBuilder.ToString());
            var visitor4 = new TestArrayVisitor2();
            visitor4.Visit(batch);
            Assert.Equal("322utf8", visitor4.stringBuilder.ToString());
        }

        [Fact]
        public void LazyStructInitialization()
        {
            StringArray stringArray = new StringArray.Builder().Append("one").AppendNull().AppendNull().Append("four").Build();
            Field stringField = new Field("column1", StringType.Default, true);
            StructType structType = new StructType(new[] { stringField });
            ArrayData structData = new ArrayData(structType, stringArray.Length, 0, 0, new[] { ArrowBuffer.Empty }, new[] { stringArray.Data });
            IArrowRecord structArray = new StructArray(structData);

            Assert.Equal(1, structArray.ColumnCount);
            Assert.Equal(structArray.Length, structArray.Column(0).Length);
        }

        private class TestTypeVisitor1 : IArrowTypeVisitor, IArrowTypeVisitor<IRecordType>
        {
            public StringBuilder stringBuilder = new StringBuilder();

            public void Visit(IArrowType type) { stringBuilder.Append(type.Name); }
            public void Visit(IRecordType type) { stringBuilder.Append('1'); VisitFields(type); }

            protected void VisitFields(IRecordType type)
            {
                for (int i = 0; i < type.FieldCount; i++) { type.GetFieldByIndex(i).DataType.Accept(this); }
            }
        }

        private class TestTypeVisitor2 : TestTypeVisitor1,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<Schema>
        {
            public void Visit(StructType type) { stringBuilder.Append('2'); VisitFields(type); }
            public void Visit(Schema type) { stringBuilder.Append('3'); VisitFields(type); }
        }

        private class TestArrayVisitor1 : IArrowArrayVisitor, IArrowArrayVisitor<IArrowRecord>
        {
            public StringBuilder stringBuilder = new StringBuilder();

            public void Visit(IArrowArray array) { stringBuilder.Append(array.Data.DataType.Name); }
            public void Visit(IArrowRecord array) { stringBuilder.Append('1'); VisitFields(array); }

            protected void VisitFields(IArrowRecord array)
            {
                for (int i = 0; i < array.ColumnCount; i++) { array.Column(i).Accept(this); }
            }
        }

        private class TestArrayVisitor2 : TestArrayVisitor1,
            IArrowArrayVisitor<StructArray>,
            IArrowArrayVisitor<RecordBatch>
        {
            public void Visit(StructArray array) { stringBuilder.Append('2'); VisitFields(array); }
            public void Visit(RecordBatch batch) { stringBuilder.Append('3'); VisitFields(batch); }
        }
    }
}
