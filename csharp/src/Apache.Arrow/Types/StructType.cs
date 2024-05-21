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
using System.Linq;

namespace Apache.Arrow.Types
{
    public sealed class StructType : NestedType, IRecordType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.Struct;
        public override string Name => "struct";

        public StructType(IReadOnlyList<Field> fields) : base(fields)
        { }

        public Field GetFieldByIndex(int index) => Fields[index];

        public Field GetFieldByName(string name,
            IEqualityComparer<string> comparer = default)
        {
            if (comparer == null)
                comparer = StringComparer.Ordinal;

            return Fields.FirstOrDefault(
                field => comparer.Equals(field.Name, name));
        }

        public int GetFieldIndex(string name,
            IEqualityComparer<string> comparer = default)
        {
            if (comparer == null)
                comparer = StringComparer.Ordinal;

            // TODO: Consider caching field index if this method is in hot path.

            for (int i = 0; i < Fields.Count; i++)
            {
                if (comparer.Equals(Fields[i].Name, name))
                {
                    return i;
                }
            }

            return -1;
        }

        public override void Accept(IArrowTypeVisitor visitor)
        {
            if (visitor is IArrowTypeVisitor<StructType> structTypeVisitor)
            {
                structTypeVisitor.Visit(this);
            }
            else if (visitor is IArrowTypeVisitor<IRecordType> interfaceVisitor)
            {
                interfaceVisitor.Visit(this);
            }
            else
            {
                visitor.Visit(this);
            }
        }

        int IRecordType.FieldCount => Fields.Count;

        Field IRecordType.GetFieldByName(string name) => GetFieldByName(name);
    }
}
