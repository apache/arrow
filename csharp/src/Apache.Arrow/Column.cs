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

namespace Apache.Arrow
{
    /// <summary>
    /// A Column data structure that logically represents a column in a dataset
    /// </summary>
    public class Column
    {
        public Field Field { get;  }
        public ChunkedArray Data { get; }

        public Column(Field field, IList<Array> arrays)
        {
            Data = new ChunkedArray(arrays);
            Field = field;
            if (!ValidateArrayDataTypes())
            {
                throw new ArgumentException($"{Field.DataType} must match {Data.DataType}");
            }
        }

        private Column(Field field, ChunkedArray arrays)
        {
            Field = field;
            Data = arrays;
        }

        public long Length => Data.Length;
        public long NullCount => Data.NullCount;
        public string Name => Field.Name;
        public IArrowType Type => Field.DataType;

        public Column Slice(int offset, int length)
        {
            return new Column(Field, Data.Slice(offset, length));
        }

        public Column Slice(int offset)
        {
            return new Column(Field, Data.Slice(offset));
        }

        private bool ValidateArrayDataTypes()
        {
            var dataTypeComparer = new ArrayDataTypeComparer(Field.DataType);

            for (int i = 0; i < Data.ArrayCount; i++)
            {
                if (Data.Array(i).Data.DataType.TypeId != Field.DataType.TypeId)
                {
                    return false;
                }

                Data.Array(i).Data.DataType.Accept(dataTypeComparer);

                if (!dataTypeComparer.DataTypeMatch)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
