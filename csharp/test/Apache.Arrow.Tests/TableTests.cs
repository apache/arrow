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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class TableTests
    {
        public static Table MakeTableWithOneColumnOfTwoIntArrays(int lengthOfEachArray)
        {
            Array intArray = ColumnTests.MakeIntArray(lengthOfEachArray);
            Array intArrayCopy = ColumnTests.MakeIntArray(lengthOfEachArray);

            Field field = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Schema s0 = new Schema.Builder().Field(field).Build();

            Column column = new Column(field, new List<IArrowArray> { intArray, intArrayCopy });
            Table table = new Table(s0, new List<Column> { column });
            return table;
        }

        [Fact]
        public void TestEmptyTable()
        {
            Table table = new Table();
            Assert.Equal(0, table.ColumnCount);
            Assert.Equal(0, table.RowCount);
        }

        [Fact]
        public void TestTableBasics()
        {
            Table table = MakeTableWithOneColumnOfTwoIntArrays(10);
            Assert.Equal(20, table.RowCount);
            Assert.Equal(1, table.ColumnCount);
            Assert.Equal("Table: 1 columns by 20 rows", table.ToString());
            Assert.Equal("ChunkedArray: Length=20, DataType=int32", table.Column(0).Data.ToString());
        }

        [Fact]
        public void TestTableFromRecordBatches()
        {
            RecordBatch recordBatch1 = TestData.CreateSampleRecordBatch(length: 10, true);
            RecordBatch recordBatch2 = TestData.CreateSampleRecordBatch(length: 10, true);
            IList<RecordBatch> recordBatches = new List<RecordBatch>() { recordBatch1, recordBatch2 };

            Table table1 = Table.TableFromRecordBatches(recordBatch1.Schema, recordBatches);
            Assert.Equal(20, table1.RowCount);
            Assert.Equal(27, table1.ColumnCount);
            Assert.Equal("ChunkedArray: Length=20, DataType=list", table1.Column(0).Data.ToString());

            FixedSizeBinaryType type = new FixedSizeBinaryType(17);
            Field newField1 = new Field(type.Name, type, false);
            Schema newSchema1 = recordBatch1.Schema.SetField(20, newField1);
            Assert.Throws<ArgumentException>(() => Table.TableFromRecordBatches(newSchema1, recordBatches));

            List<Field> fields = new List<Field>();
            Field.Builder fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Ints").DataType(Int32Type.Default).Nullable(true).Build());
            fieldBuilder = new Field.Builder();
            fields.Add(fieldBuilder.Name("Strings").DataType(StringType.Default).Nullable(true).Build());
            StructType structType = new StructType(fields);

            Field newField2 = new Field(structType.Name, structType, false);
            Schema newSchema2 = recordBatch1.Schema.SetField(16, newField2);
            Assert.Throws<ArgumentException>(() => Table.TableFromRecordBatches(newSchema2, recordBatches));
        }

        [Fact]
        public void TestTableAddRemoveAndSetColumn()
        {
            Table table = MakeTableWithOneColumnOfTwoIntArrays(10);
            Assert.Equal("Table: 1 columns by 20 rows", table.ToString());
            Assert.Equal("Field: Name=f0, DataType=int32, IsNullable=True, Metadata count=0", table.Column(0).Field.ToString());
            Assert.Equal("ChunkedArray: Length=20, DataType=int32", table.Column(0).Data.ToString());

            Array nonEqualLengthIntArray = ColumnTests.MakeIntArray(10);
            Field field1 = new Field.Builder().Name("f1").DataType(Int32Type.Default).Build();
            Column nonEqualLengthColumn = new Column(field1, new IArrowArray[] { nonEqualLengthIntArray });
            Assert.Throws<ArgumentException>(() => table.InsertColumn(-1, nonEqualLengthColumn));
            Assert.Throws<ArgumentException>(() => table.InsertColumn(1, nonEqualLengthColumn));

            Array equalLengthIntArray = ColumnTests.MakeIntArray(20);
            Field field2 = new Field.Builder().Name("f2").DataType(Int32Type.Default).Build();
            Column equalLengthColumn = new Column(field2, new IArrowArray[] { equalLengthIntArray });
            Column existingColumn = table.Column(0);

            Table newTable = table.InsertColumn(0, equalLengthColumn);
            Assert.Equal(2, newTable.ColumnCount);
            Assert.True(newTable.Column(0) == equalLengthColumn);
            Assert.True(newTable.Column(1) == existingColumn);

            newTable = newTable.RemoveColumn(1);
            Assert.Equal(1, newTable.ColumnCount);
            Assert.True(newTable.Column(0) == equalLengthColumn);

            newTable = table.SetColumn(0, existingColumn);
            Assert.True(newTable.Column(0) == existingColumn);
        }

        [Fact]
        public void TestBuildFromRecordBatch()
        {
            Schema.Builder builder = new Schema.Builder();
            builder.Field(new Field("A", Int64Type.Default, nullable: false));
            Schema schema = builder.Build();

            RecordBatch batch = TestData.CreateSampleRecordBatch(schema, 10);
            Table table = Table.TableFromRecordBatches(schema, new[] { batch });

            Assert.NotNull(table.Column(0).Data.ArrowArray(0) as Int64Array);
        }
    }

}
