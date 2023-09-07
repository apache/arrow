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
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero.Tests
{
    public class DeclarationTests
    {
        private readonly ITestOutputHelper _output;

        public DeclarationTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestRecordBatchSource()
        {
            // arrange
            var recordBatch = TestData.GetCustomersRecordBatch();

            var recordBatchSource = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(recordBatch));

            // act
            var schema = TestData.GetCustomersSchema();
            var result = recordBatchSource.ToRecordBatchReader(schema);

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                customerId | firstName  | lastName   | 
                c1         | Luke       | Skywalker  | 
                c2         | Princess   | Leia       | 
                c3         | Obi-Wan    | Kenobi     | 
                """, rowCount: 3);
        }

        private void AssertTable(Table table, string expected, int? rowCount = null, int columnPadding = 10)
        {
            var actual = PrintPrintTable(table, columnPadding);

            _output.WriteLine(actual);

            Assert.Equal(expected, actual);

            if (rowCount.HasValue)
                Assert.Equal(rowCount.Value, table.RowCount);
        }

        [Fact]
        public async Task TestHashJoin()
        {
            // arrange
            var left = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(TestData.GetCustomersRecordBatch()));

            var right = new Declaration("record_batch_reader_source",
                new RecordBatchReaderSourceNodeOptions(TestData.GetOrdersRecordBatchStream()));

            var hashJoinOptions = new HashJoinNodeOptions(
                GArrowJoinType.GARROW_JOIN_TYPE_INNER,
                new string[] { "customerId" },
                new string[] { "customerId" });

            var hashJoin = new Declaration("hashjoin", 
                options: hashJoinOptions, inputs: new List<Declaration> { left, right });

            // act
            var result = hashJoin.ToRecordBatchReader(
                new Schema.Builder()
                    .Field(new Field("customerId", StringType.Default, true))
                    .Field(new Field("firstName", StringType.Default, true))
                    .Field(new Field("lastName", StringType.Default, true))
                    .Field(new Field("orderId", StringType.Default, true))
                    .Field(new Field("customerId", StringType.Default, true))
                    .Field(new Field("productId", StringType.Default, true))
                .Build());

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                customerId | firstName  | lastName   | orderId    | customerId | productId  | 
                c1         | Luke       | Skywalker  | o1         | c1         | p1         | 
                c2         | Princess   | Leia       | o2         | c2         | p2         | 
                c3         | Obi-Wan    | Kenobi     | o3         | c3         | p3         | 
                """, rowCount: 3);
        }

        [Fact]
        public async Task TestSingleFilter()
        {
            // arrange
            var recordBatch = TestData.GetCustomersRecordBatch();

            var recordBatchSource = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(recordBatch));

            var orderBy = Declaration.FromSequence(new List<Declaration> {
                recordBatchSource,
                new Declaration("filter", new FilterNodeOptions(
                    new Equal(new FieldExpression("firstName"), new LiteralExpression("Luke"))
                ))
            });

            // act
            var schema = TestData.GetCustomersSchema();
            var result = orderBy.ToRecordBatchReader(schema);

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                customerId | firstName  | lastName   | 
                c1         | Luke       | Skywalker  | 
                """, rowCount: 1);
        }

        [Fact]
        public async Task TestMultiFilter()
        {
            // arrange
            var recordBatch = TestData.GetCustomersRecordBatch();

            var recordBatchSource = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(recordBatch));

            var orderBy = Declaration.FromSequence(new List<Declaration> {
                recordBatchSource,
                new Declaration("filter", new FilterNodeOptions(
                    new Or(
                        new Equal(new FieldExpression("firstName"), new LiteralExpression("Luke")),
                        new Equal(new FieldExpression("lastName"), new LiteralExpression("Kenobi"))
                    )))
            });

            // act
            var schema = TestData.GetCustomersSchema();
            var result = orderBy.ToRecordBatchReader(schema);

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                customerId | firstName  | lastName   | 
                c1         | Luke       | Skywalker  | 
                c3         | Obi-Wan    | Kenobi     | 
                """, rowCount: 2);
        }

        [Fact]
        public async Task TestUnion()
        {
            // arrange
            var left = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(TestData.GetCustomersRecordBatch()));

            var right = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(TestData.GetCustomersRecordBatch()));

            var union = new Declaration("union", inputs: new List<Declaration> { left, right });

            // act
            var schema = TestData.GetCustomersSchema();
            var result = union.ToRecordBatchReader(schema);

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                customerId | firstName  | lastName   | 
                c1         | Luke       | Skywalker  | 
                c2         | Princess   | Leia       | 
                c3         | Obi-Wan    | Kenobi     | 
                c1         | Luke       | Skywalker  | 
                c2         | Princess   | Leia       | 
                c3         | Obi-Wan    | Kenobi     | 
                """, rowCount: 6);
        }

        [Fact]
        public async Task TestProjection()
        {
            // arrange
            var customers = new Declaration("record_batch_source",
                new RecordBatchSourceNodeOptions(TestData.GetCustomersRecordBatch()));

            var project = Declaration.FromSequence(new List<Declaration> {
                customers,
                new Declaration("project", new ProjectNodeOptions(
                    new List<Expression> {
                        new Function(
                            "binary_join_element_wise",
                             new FieldExpression("lastName"),
                             new FieldExpression("firstName"),
                             new LiteralExpression(", ")
                        )
                    },
                    new List<string> { "fullName" }))
            });

            // act
            var result = project.ToRecordBatchReader(
                new Schema.Builder()
                    .Field(new Field("fullName", StringType.Default, true))
                .Build());

            // assert
            var table = await ConvertStreamToTable(result);

            AssertTable(table,
                """
                fullName        | 
                Skywalker, Luke | 
                Leia, Princess  | 
                Kenobi, Obi-Wan | 
                """, rowCount: 3, columnPadding: 15);
        }

        private async Task<Table> ConvertStreamToTable(IArrowArrayStream result)
        {
            Schema schema = null;

            var recordBatches = new List<RecordBatch>();
            
            while (true)
            {
                var recordBatch = await result.ReadNextRecordBatchAsync();
                if (recordBatch == null) break;

                if (schema == null)
                    schema = recordBatch.Schema;

                recordBatches.Add(recordBatch);
            }

            return Table.TableFromRecordBatches(schema, recordBatches);
        }

        public static string PrintPrintTable(Table table, int columnPadding = 10)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < table.ColumnCount; i++)
            {
                sb.Append(table.Column(i).Name.PadRight(columnPadding) + " | ");
            }

            sb.AppendLine();

            for (var i = 0; i < table.RowCount; i++)
            {
                for (var j = 0; j < table.ColumnCount; j++)
                {
                    var sliced = table.Column(j).Slice(i, 1);

                    for (var k = 0; k < sliced.Data.ArrayCount; k++)
                    {
                        if (sliced.Data.Array(k).Length == 0)
                            continue;

                        var data = sliced.Data.Array(k);
                        string value;

                        switch (data)
                        {
                            case StringArray stringArray:
                                value = stringArray.GetString(0);
                                break;

                            case Int32Array int32Array:
                                value = int32Array.GetValue(0).ToString();
                                break;

                            default:
                                throw new Exception("Array type not supported");
                        }

                        sb.Append(value.PadRight(columnPadding) + " | ");
                    }
                }
                sb.AppendLine();
            }

            return sb.ToString().Trim('\n').Trim('\r');
        }

        public class TestResult
        {
            public string TableAsString { get; set; }
            public int RowCount { get; set; }
        }
    }
}
