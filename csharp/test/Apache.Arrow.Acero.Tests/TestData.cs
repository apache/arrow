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

using System.Collections.Generic;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Acero.Tests
{
    public static class TestData
    {
        public static Schema GetCustomersSchema()
        {
            return new Schema.Builder()
                .Field(new Field("customerId", StringType.Default, true))
                .Field(new Field("firstName", StringType.Default, true))
                .Field(new Field("lastName", StringType.Default, true))
                .Build();
        }

        public static RecordBatch GetCustomersRecordBatch()
        {
            var schema = GetCustomersSchema();

            return new RecordBatch(schema, new[]
                {
                    new StringArray.Builder().AppendRange(new[] { "c1", "c2", "c3", "c4" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "Luke", "Princess", "Obi-Wan", "Darth" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "Skywalker", "Leia", "Kenobi", "Vader" }).Build(),
                }, 3);
        }

        public static Schema GetOrdersSchema()
        {
            return new Schema.Builder()
                .Field(new Field("orderId", StringType.Default, true))
                .Field(new Field("customerId", StringType.Default, true))
                .Field(new Field("productId", StringType.Default, true))
                .Build();
        }

        public static IArrowArrayStream GetOrdersRecordBatchStream()
        {
            var schema = GetOrdersSchema();

            return new AsyncRecordBatchStream(GetOrdersRecordBatchStreamInternal(), schema);
        }

        public static async IAsyncEnumerable<RecordBatch> GetOrdersRecordBatchStreamInternal()
        {
            var schema = GetOrdersSchema();

            yield return new RecordBatch(schema, new[]
                {
                    new StringArray.Builder().AppendRange(new[] { "o1", "o2", "o3" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "c1", "c2", "c3" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "p1", "p2", "p3" }).Build(),
                }, 3);

            yield return new RecordBatch(schema, new[]
                {
                    new StringArray.Builder().AppendRange(new[] { "o4", "o5", "o6" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "c4", "c5", "c6" }).Build(),
                    new StringArray.Builder().AppendRange(new[] { "p4", "p5", "p6" }).Build(),
                }, 3);
        }
    }
}
