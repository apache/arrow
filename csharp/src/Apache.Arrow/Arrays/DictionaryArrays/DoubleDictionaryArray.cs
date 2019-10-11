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
