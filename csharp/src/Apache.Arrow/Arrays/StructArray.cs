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

namespace Apache.Arrow
{
    public class StructArray : Array
    {
        private readonly List<Array> _fields;

        public IEnumerable<Array> Fields => _fields;

        public StructArray(
            IArrowType dataType, int length,
            IEnumerable<Array> children,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
        : this(new ArrayData(
            dataType, length, nullCount, offset, new[] { nullBitmapBuffer }, 
            children.Select(child => child.Data)))
        { }

        public StructArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Struct);

            _fields = new List<Array>();
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    }
}
