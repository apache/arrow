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

namespace Apache.Arrow
{
    /// <summary>
    /// A logical Table class to represent a dataset as a sequence of Columns
    /// </summary>
    public class Table
    {
        public Schema Schema { get; }
        public long RowCount { get; }
        public int ColumnCount { get; private set; }
        public Column Column(int columnIndex) => _columns[columnIndex];

        private readonly IList<Column> _columns;
        public static Table TableFromRecordBatches(Schema schema, IList<RecordBatch> recordBatches)
        {
            int nBatches = recordBatches.Count;
            int nColumns = schema.FieldsList.Count;

            List<Column> columns = new List<Column>(nColumns);
            for (int icol = 0; icol < nColumns; icol++)
            {
                List<Array> columnArrays = new List<Array>(nBatches);
                for (int jj = 0; jj < nBatches; jj++)
                {
                    columnArrays.Add(recordBatches[jj].Column(icol) as Array);
                }
                columns.Add(new Column(schema.GetFieldByIndex(icol), columnArrays));
            }

            return new Table(schema, columns);
        }

        public Table(Schema schema, IList<Column> columns)
        {
            Schema = schema;
            _columns = columns;
            if (columns.Count > 0)
            {
                RowCount = columns[0].Length;
                ColumnCount = columns.Count;
            }
        }

        public Table()
        {
            Schema = new Schema.Builder().Build();
            _columns = new List<Column>();
        }

        public Table RemoveColumn(int columnIndex)
        {
            Schema newSchema = Schema.RemoveField(columnIndex);
            IList<Column> newColumns = Utility.DeleteListElement(_columns, columnIndex);
            return new Table(newSchema, newColumns);
        }

        public Table InsertColumn(int columnIndex, Column column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex > _columns.Count)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed into Table.AddColumn");
            }
            if (column.Length != RowCount)
            {
                throw new ArgumentException($"Column's length {column.Length} must match Table's length {RowCount}");
            }

            Schema newSchema = Schema.InsertField(columnIndex, column.Field);
            IList<Column> newColumns = Utility.AddListElement(_columns, columnIndex, column);
            return new Table(newSchema, newColumns);
        }

        public Table SetColumn(int columnIndex, Column column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex >= ColumnCount)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed in to Table.SetColumn");
            }

            if (column.Length != RowCount)
            {
                throw new ArgumentException($"Column's length {column.Length} must match table's length {RowCount}");
            }

            Schema newSchema = Schema.SetField(columnIndex, column.Field);
            IList<Column> newColumns = Utility.SetListElement(_columns, columnIndex, column);
            return new Table(newSchema, newColumns);
        }

        // TODO: Flatten for Tables with Lists/Structs?
    }
}
