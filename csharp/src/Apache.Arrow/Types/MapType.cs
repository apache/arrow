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

namespace Apache.Arrow.Types
{
    public sealed class MapType : NestedType // MapType = ListType(StructType("key", "value")) 
    {
        private const string EntriesKey = "entries";
        private const string KeyKey = "key";
        private const string ValueKey = "value";

        public override ArrowTypeId TypeId => ArrowTypeId.Map;
        public override string Name => "map";
        public readonly bool KeySorted;

        public StructType KeyValueType => Fields[0].DataType as StructType;
        public Field KeyField => KeyValueType.Fields[0];
        public Field ValueField => KeyValueType.Fields[1];

        public MapType(IArrowType key, IArrowType value, bool nullable = true, bool keySorted = false)
            : base(Entries(key, value, nullable))
        {
            KeySorted = keySorted;
        }

        public MapType(Field key, Field value, bool keySorted = false)
            : base(Entries(key, value))
        {
            KeySorted = keySorted;
        }

        public MapType(StructType entries, bool keySorted = false) : base(Entries(entries))
        {
            KeySorted = keySorted;
        }

        public MapType(Field entries, bool keySorted = false) : base(Entries(entries))
        {
            KeySorted = keySorted;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        public MapType UnsortedKey()
        {
            if (!KeySorted) { return this; }

            return new MapType(Fields[0], keySorted: false);
        }

        private static Field Entries(IArrowType key, IArrowType value, bool nullable) =>
            new Field(EntriesKey, NewStruct(new Field(KeyKey, key, false), new Field(ValueKey, value, nullable)), false);

        private static Field Entries(Field key, Field value) =>
            new Field(EntriesKey, NewStruct(NamedField(KeyKey, key), NamedField(ValueKey, value)), false);

        private static Field Entries(StructType entries)
        {
            return new Field(EntriesKey, Struct(entries), false);
        }

        private static StructType NewStruct(Field key, Field value) => new StructType(new[] { key, value });

        private static StructType Struct(StructType entries)
        {
            Field key = NamedField(KeyKey, entries.Fields[0]);
            Field value = NamedField(ValueKey, entries.Fields[1]);
            return object.ReferenceEquals(key, entries.Fields[0]) && object.ReferenceEquals(value, entries.Fields[1]) ? entries : NewStruct(key, value);
        }

        private static Field Entries(Field entries)
        {
            StructType structType = (StructType)entries.DataType;
            StructType adjustedStruct = Struct(structType);
            return StringComparer.Ordinal.Equals(entries.Name, EntriesKey) && object.ReferenceEquals(structType, adjustedStruct) ? entries : new Field(EntriesKey, adjustedStruct, false);
        }

        private static Field NamedField(string name, Field field)
        {
            return StringComparer.Ordinal.Equals(name, field.Name) ? field : new Field(name, field.DataType, field.IsNullable, field.Metadata);
        }
    }
}
