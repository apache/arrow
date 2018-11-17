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
using System;

namespace Apache.Arrow
{
    public class ArrowArrayFactory
    {
        private class FactoryTypeVisitor :
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<BinaryType>
        {
            private readonly ArrayData _data;
            private IArrowArray _array;

            public FactoryTypeVisitor(ArrayData data)
            {
                _data = data;
            }

            public IArrowArray CreateArray()
            {
                _data.DataType.Accept(this);
                return _array;
            }

            public void Visit(Int8Type type) => _array = new Int8Array(_data);
            public void Visit(Int16Type type) => _array = new Int16Array(_data);
            public void Visit(Int32Type type) => _array = new Int32Array(_data);
            public void Visit(Int64Type type) => _array = new Int64Array(_data);
            public void Visit(UInt8Type type) => _array = new UInt8Array(_data);
            public void Visit(UInt16Type type) => _array = new UInt16Array(_data);
            public void Visit(UInt32Type type) => _array = new UInt32Array(_data);
            public void Visit(UInt64Type type) => _array = new UInt64Array(_data);
            public void Visit(BooleanType type) => _array = new BooleanArray(_data);
            public void Visit(FloatType type) => _array = new FloatArray(_data);
            public void Visit(DoubleType type) => _array = new DoubleArray(_data);
            public void Visit(StructType type) => _array = new StructArray(_data);
            public void Visit(UnionType type) => _array = new UnionArray(_data);
            public void Visit(ListType type) => _array = new ListArray(_data);
            public void Visit(TimestampType type) => _array = new TimestampArray(_data);
            public void Visit(BinaryType type) => _array = new BinaryArray(_data);
            public void Visit(StringType type) => _array = new StringArray(_data);

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }
        }

        public static IArrowArray BuildArray(ArrayData data)
        {
            var visitor = new FactoryTypeVisitor(data);
            var array = visitor.CreateArray();
            return array;
        }
    }
}
