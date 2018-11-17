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
    public class UnionArray: Array
    {
        public UnionType Type => Data.DataType as UnionType;

        public UnionMode Mode => Type.Mode;

        public ArrowBuffer TypeBuffer => Data.Buffers[1];

        public ArrowBuffer ValueOffsetBuffer => Data.Buffers[2];

        public ReadOnlySpan<byte> TypeIds => TypeBuffer.GetSpan<byte>();

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetBuffer.GetSpan<int>();

        public UnionArray(ArrayData data) 
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Union);
            data.EnsureBufferCount(3);
        }

        public IArrowArray GetChild(int index)
        {
            // TODO: Implement
            throw new NotImplementedException();
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    }
}
