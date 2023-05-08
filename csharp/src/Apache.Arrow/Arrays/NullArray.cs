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
    public class NullArray : IArrowArray
    {
        public ArrayData Data { get; }

        public NullArray(ArrayData data)
        {
            data.EnsureDataType(ArrowTypeId.Null);
            data.EnsureBufferCount(0);
        }

        public int Length => Data.Length;

        public int Offset => Data.Offset;

        public int NullCount => Data.NullCount;

        public void Dispose() { }
        public bool IsNull(int index) => true;
        public bool IsValid(int index) => false;

        public void Accept(IArrowArrayVisitor visitor) => throw new System.NotImplementedException();
    }
}
