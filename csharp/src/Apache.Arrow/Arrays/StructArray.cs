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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Apache.Arrow
{
    public class StructArray : Array, IArrowRecord
    {
        private IReadOnlyList<IArrowArray> _fields;

        public IReadOnlyList<IArrowArray> Fields =>
            LazyInitializer.EnsureInitialized(ref _fields, () => InitializeFields());

        public StructArray(
            IArrowType dataType, int length,
            IEnumerable<IArrowArray> children,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(
                dataType, length, nullCount, offset, new[] { nullBitmapBuffer },
                children.Select(child => child.Data)))
        {
            _fields = children.ToArray();
        }

        public StructArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Struct);
        }

        public override void Accept(IArrowArrayVisitor visitor)
        {
            switch (visitor)
            {
                case IArrowArrayVisitor<StructArray> structArrayVisitor:
                    structArrayVisitor.Visit(this);
                    break;
                case IArrowArrayVisitor<IArrowRecord> arrowStructVisitor:
                    arrowStructVisitor.Visit(this);
                    break;
                default:
                    visitor.Visit(this);
                    break;
            }
        }

        private IReadOnlyList<IArrowArray> InitializeFields()
        {
            IArrowArray[] result = new IArrowArray[Data.Children.Length];
            for (int i = 0; i < Data.Children.Length; i++)
            {
                result[i] = ArrowArrayFactory.BuildArray(Data.Children[i]);
            }
            return result;
        }

        IRecordType IArrowRecord.Schema => (StructType)Data.DataType;

        int IArrowRecord.ColumnCount => Fields.Count;

        IArrowArray IArrowRecord.Column(string columnName, IEqualityComparer<string> comparer) =>
            Fields[((StructType)Data.DataType).GetFieldIndex(columnName, comparer)];

        IArrowArray IArrowRecord.Column(int columnIndex) => Fields[columnIndex];
    }
}
