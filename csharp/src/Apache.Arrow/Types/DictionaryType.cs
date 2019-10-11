using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Types
{
    public sealed class DictionaryType : ArrowType
    {
        DictionaryType(ArrowTypeId containedTypeId)
        {
            ContainedTypeId = containedTypeId;
        }
        public static DictionaryType Default(ArrowTypeId containedTypeId) => new DictionaryType(containedTypeId);

        public override ArrowTypeId TypeId => ArrowTypeId.Dictionary;
        public ArrowTypeId ContainedTypeId { get; }
        public override string Name => "dictionary";

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
