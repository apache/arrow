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
        public Schema Schema;
        public long NumRows;
        public int NumColumns;
        public Column Column(int columnIndex) => _columns[columnIndex];

        private readonly IList<Column> _columns;
        public static Table TableFromRecordBatches(Schema schema, IList<RecordBatch> recordBatches)
        {
            int nBatches = recordBatches.Count;
            int nColumns = schema.Fields.Count;

            List<Column> columns = new List<Column>(nColumns);
            List<Array> columnArrays = new List<Array>(nBatches);
            for (int icol = 0; icol < nColumns; icol++)
            {
                for (int jj = 0; jj < nBatches; jj++)
                {
                    columnArrays.Add(recordBatches[jj].Column(icol) as Array);
                }
                columns.Add(new Arrow.Column(schema.GetFieldByIndex(icol), columnArrays));
                columnArrays.Clear();
            }
            return new Table(schema, columns);
        }

        public Table(Schema schema, IList<Column> columns)
        {
            Schema = schema;
            _columns = columns;
            if (columns.Count > 0)
            {
                NumRows = columns[0].Length;
                NumColumns = columns.Count;
            }
        }

        public Table()
        {
            Schema = new Schema.Builder().Build();
            _columns = new List<Column>();
        }


        public void RemoveColumn(int columnIndex)
        {
            // Does not return a new table.
            Schema.RemoveField(columnIndex);
            _columns.RemoveAt(columnIndex);
            NumColumns--;
        }

        public void InsertColumn(int columnIndex, Column column)
        {
            // Does not return a new table
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex > _columns.Count)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed into Table.AddColumn");
            }
            if (column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match Table's length {NumRows}");
            }
            Schema.InsertField(columnIndex, column.Field);
            _columns.Insert(columnIndex, column);
            NumColumns++;
        }

        public void SetColumn(int columnIndex, Column column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex >= NumColumns)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed in to Table.SetColumn");
            }
            if (column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match table's length {NumRows}");
            }
            Schema.SetField(columnIndex, column.Field);
            _columns[columnIndex] = column;
        }

        // TODO: Flatten for Tables with Lists/Structs?
    }
}
