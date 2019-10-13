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
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays.DictionaryArrays
{
    public class DoubleDictionaryArray : PrimitiveDictionaryArray<double>, IArrowArray
    {
        public class Builder : PrimitiveDictionaryArrayBuilder<double, DoubleDictionaryArray, Builder>
        {
            public Builder() : base(null, null) { }

            public Builder(IEqualityComparer<double> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
            {
            }

            public override DoubleDictionaryArray Build(MemoryAllocator allocator)
            {
                allocator = allocator ?? MemoryAllocator.Default.Value;

                return new DoubleDictionaryArray(IndicesBuffer.Length, ValuesBuffer.Length, IndicesBuffer.Build(allocator), ValuesBuffer.Build(allocator),
                    ArrowBuffer.Empty);
            }
        }

        public DoubleDictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
        }

        public DoubleDictionaryArray(int length, int uniqueValuesCount, ArrowBuffer indices, ArrowBuffer dataBuffer, ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0) :
            this(new ArrayData(DictionaryType.Default(ArrowTypeId.Double), length, nullCount, offset, new[] { nullBitmapBuffer, indices, dataBuffer }), uniqueValuesCount)
        {
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    }
}
