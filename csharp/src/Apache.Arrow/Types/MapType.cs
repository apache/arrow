using System.Collections.Generic;

namespace Apache.Arrow.Types
{
    public sealed class MapType : ListType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.Map;
        public override string Name => "map";

        public StructType KeyValueType => Fields[0].DataType as StructType;
        public Field KeyField => KeyValueType.Fields[0];
        public new Field ValueField => KeyValueType.Fields[1];

        public IArrowType KeyDataType => Fields[0].DataType;
        public new IArrowType ValueDataType => Fields[1].DataType;

        public MapType(IArrowType key, IArrowType value)
            : this(key, value, true)
        {
        }

        public MapType(IArrowType key, IArrowType value, bool nullable)
            : this(new Field("key", key, false), new Field("value", value, nullable))
        {
        }

        public MapType(Field key, Field value)
            : this(new StructType(new List<Field>() { key, value }))
        {
        }

        public MapType(StructType entries) : this(new Field("entries", entries, false))
        {
        }

        public MapType(Field keyvalue) : base(keyvalue)
        {
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
