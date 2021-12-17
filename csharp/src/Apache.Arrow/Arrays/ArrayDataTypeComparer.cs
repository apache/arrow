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

using System.Diagnostics;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class ArrayTypeComparer :
        IArrowTypeVisitor<TimestampType>,
        IArrowTypeVisitor<Date32Type>,
        IArrowTypeVisitor<Date64Type>,
        IArrowTypeVisitor<Time32Type>,
        IArrowTypeVisitor<Time64Type>,
        IArrowTypeVisitor<FixedSizeBinaryType>,
        IArrowTypeVisitor<ListType>,
        IArrowTypeVisitor<StructType>
    {
        private readonly IArrowType _expectedType;
        private bool _dataTypeMatch;

        public ArrayTypeComparer(IArrowType expectedType)
        {
            _expectedType = expectedType;
            _dataTypeMatch = false;
        }

        public bool DataTypeMatch => _dataTypeMatch;

        public void Visit(TimestampType actualType)
        {
            var expectedType = (TimestampType)_expectedType;

            if (expectedType.Timezone == actualType.Timezone && expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Date32Type actualType)
        {
            var expectedType = (Date32Type)_expectedType;

            if (expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Date64Type actualType)
        {
            var expectedType = (Date64Type)_expectedType;

            if (expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Time32Type actualType)
        {
            var expectedType = (Time32Type)_expectedType;

            if (expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(Time64Type actualType)
        {
            var expectedType = (Time64Type)_expectedType;

            if (expectedType.Unit == actualType.Unit)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(FixedSizeBinaryType actualType)
        {
            var expectedType = (FixedSizeBinaryType)_expectedType;

            if (expectedType.ByteWidth == actualType.ByteWidth)
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(ListType actualType)
        {
            var expectedType = (ListType)_expectedType;

            if (CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public void Visit(StructType actualType)
        {
            var expectedType = (StructType)_expectedType;

            if (CompareNested(expectedType, actualType))
            {
                _dataTypeMatch = true;
            }
        }

        public static bool Compare(Field expected, Field actual)
        {
            if (ReferenceEquals(expected, actual))
            {
                return true;
            }

            if (expected.DataType.TypeId != actual.DataType.TypeId)
            {
                return false;
            }

            var dataTypeVistor = new ArrayTypeComparer(expected.DataType);

            actual.DataType.Accept(dataTypeVistor);

            if (!dataTypeVistor.DataTypeMatch)
            {
                return false;
            }

            return true;
        }

        private static bool CompareNested(NestedType expectedType, NestedType actualType)
        {
            for (int i = 0; i < expectedType.Fields.Count; i++)
            {
                return Compare(expectedType.Fields[i], actualType.Fields[i]);
            }

            return true;   
        }

        public void Visit(IArrowType actualType)
        {
            _dataTypeMatch = true;
        }
    }
}
