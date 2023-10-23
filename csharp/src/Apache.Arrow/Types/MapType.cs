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

using System.Collections.Generic;

namespace Apache.Arrow.Types
{
    public sealed class MapType : NestedType // MapType = ListType(StructType("key", "value")) 
    {
        public override ArrowTypeId TypeId => ArrowTypeId.Map;
        public override string Name => "map";
        public readonly bool KeySorted;

        public StructType KeyValueType => Fields[0].DataType as StructType;
        public Field KeyField => KeyValueType.Fields[0];
        public Field ValueField => KeyValueType.Fields[1];

        public MapType(IArrowType key, IArrowType value, bool nullable = true, bool keySorted = false)
            : this(new Field("key", key, false), new Field("value", value, nullable), keySorted)
        {
        }

        public MapType(Field key, Field value, bool keySorted = false)
            : this(new StructType(new List<Field>() { key, value }), keySorted)
        {
        }

        public MapType(StructType entries, bool keySorted = false) : this(new Field("entries", entries, false), keySorted)
        {
        }

        public MapType(Field entries, bool keySorted = false) : base(entries)
        {
            KeySorted = keySorted;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        public MapType UnsortedKey()
        {
            if (!KeySorted) { return this; }

            return new MapType(Fields[0], keySorted: false);
        }
    }
}
