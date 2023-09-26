using System.Collections.Generic;

namespace Apache.Arrow.Types
{
    public sealed class MapType : NestedType // MapType = ListType(StrucType("key", "value")) 
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
