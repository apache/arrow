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
using Xunit;

namespace Apache.Arrow.Tests
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

        public ArrayTypeComparer(IArrowType expectedType)
        {
            Debug.Assert(expectedType != null);
            _expectedType = expectedType;
        }

        public void Visit(TimestampType actualType)
        {
            Assert.IsAssignableFrom<TimestampType>(_expectedType);

            var expectedType = (TimestampType)_expectedType;

            Assert.Equal(expectedType.Timezone, actualType.Timezone);
            Assert.Equal(expectedType.Unit, actualType.Unit);
        }

        public void Visit(Date32Type actualType)
        {
            Assert.IsAssignableFrom<Date32Type>(_expectedType);
            var expectedType = (Date32Type)_expectedType;

            Assert.Equal(expectedType.Unit, actualType.Unit);
        }

        public void Visit(Date64Type actualType)
        {
            Assert.IsAssignableFrom<Date64Type>(_expectedType);
            var expectedType = (Date64Type)_expectedType;

            Assert.Equal(expectedType.Unit, actualType.Unit);
        }

        public void Visit(Time32Type actualType)
        {
            Assert.IsAssignableFrom<Time32Type>(_expectedType);
            var expectedType = (Time32Type)_expectedType;

            Assert.Equal(expectedType.Unit, actualType.Unit);
        }

        public void Visit(Time64Type actualType)
        {
            Assert.IsAssignableFrom<Time64Type>(_expectedType);
            var expectedType = (Time64Type)_expectedType;

            Assert.Equal(expectedType.Unit, actualType.Unit);
        }

        public void Visit(FixedSizeBinaryType actualType)
        {
            Assert.IsAssignableFrom<FixedSizeBinaryType>(_expectedType);
            var expectedType = (FixedSizeBinaryType)_expectedType;

            Assert.Equal(expectedType.ByteWidth, actualType.ByteWidth);
        }

        public void Visit(ListType actualType)
        {
            Assert.IsAssignableFrom<ListType>(_expectedType);
            var expectedType = (ListType)_expectedType;

            CompareNested(expectedType, actualType);
        }

        public void Visit(StructType actualType)
        {
            Assert.IsAssignableFrom<StructType>(_expectedType);
            var expectedType = (StructType)_expectedType;

            CompareNested(expectedType, actualType);
        }

        private static void CompareNested(NestedType expectedType, NestedType actualType)
        {
            Assert.Equal(expectedType.Fields.Count, actualType.Fields.Count);

            for (int i = 0; i < expectedType.Fields.Count; i++)
            {
                FieldComparer.Compare(expectedType.Fields[i], actualType.Fields[i]);
            }
        }

        public void Visit(IArrowType actualType)
        {
            Assert.IsAssignableFrom(actualType.GetType(), _expectedType);
        }
    }
}
