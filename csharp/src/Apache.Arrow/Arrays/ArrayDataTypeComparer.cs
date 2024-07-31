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

namespace Apache.Arrow
{
    internal sealed class ArrayDataTypeComparer :
        IArrowTypeVisitor<TimestampType>,
        IArrowTypeVisitor<Date32Type>,
        IArrowTypeVisitor<Date64Type>,
        IArrowTypeVisitor<TimeBasedType>,
        IArrowTypeVisitor<FixedSizeBinaryType>,
        IArrowTypeVisitor<ListType>,
        IArrowTypeVisitor<FixedSizeListType>,
        IArrowTypeVisitor<StructType>,
        IArrowTypeVisitor<UnionType>,
        IArrowTypeVisitor<MapType>,
        IArrowTypeVisitor<IntervalType>
    {
        private readonly IArrowType _expectedType;
        private bool _dataTypeMatch;

        public ArrayDataTypeComparer(IArrowType expectedType)
        {
            _expectedType = expectedType;
        }

        public bool DataTypeMatch => _dataTypeMatch;

        public void Visit(TimestampType actualType)
        {
            if (_expectedType is TimestampType expectedType
                && expectedType.Timezone == actualType.Timezone
                && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Date32Type actualType)
        {
            if (_expectedType is Date32Type expectedType
                && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Date64Type actualType)
        {
            if (_expectedType is Date64Type expectedType
                && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(TimeBasedType actualType)
        {
            if (_expectedType.TypeId == actualType.TypeId
                && _expectedType is TimeBasedType expectedType
                && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(FixedSizeBinaryType actualType)
        {
            if (_expectedType is FixedSizeBinaryType expectedType
                && expectedType.ByteWidth == actualType.ByteWidth)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(ListType actualType)
        {
            if (_expectedType is ListType expectedType
                && CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(FixedSizeListType actualType)
        {
            if (_expectedType is FixedSizeListType expectedType
                && actualType.ListSize == expectedType.ListSize
                && CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(StructType actualType)
        {
            if (_expectedType is StructType expectedType
                && CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(UnionType actualType)
        {
            if (_expectedType is UnionType expectedType
                && CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(MapType actualType)
        {
            if (_expectedType is MapType expectedType
                && expectedType.KeySorted == actualType.KeySorted
                && CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(IntervalType actualType)
        {
            if (_expectedType is IntervalType expectedType
                && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        private static bool CompareNested(NestedType expectedType, NestedType actualType)
        {
            if (expectedType.Fields.Count != actualType.Fields.Count)
            {
                return false;
            }

            for (int i = 0; i < expectedType.Fields.Count; i++)
            {
                if (expectedType.Fields[i].DataType.TypeId != actualType.Fields[i].DataType.TypeId)
                {
                    return false;
                }

                var dataTypeMatch = FieldComparer.Compare(expectedType.Fields[i], actualType.Fields[i]);

                if (!dataTypeMatch)
                {
                    return false;
                }
            }

            return true;   
        }

        public void Visit(IArrowType actualType)
        {
            if (_expectedType.TypeId == actualType.TypeId)
            {
                _dataTypeMatch = true;
            }
        }
    }
}
