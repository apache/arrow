using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays.DictionaryArrays
{
    public class Int64DictionaryArray : PrimitiveDictionaryArray<long>
    {
        public class Builder : PrimitiveDictionaryArrayBuilder<long, Int64DictionaryArray, Builder>
        {
            public Builder() : base(null, null) { }

            public Builder(IEqualityComparer<long> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
            {
            }

            public override Int64DictionaryArray Build(MemoryAllocator allocator)
            {
                allocator = allocator ?? MemoryAllocator.Default.Value;

                return new Int64DictionaryArray(IndicesBuffer.Length, ValuesBuffer.Length, IndicesBuffer.Build(allocator), ValuesBuffer.Build(allocator),
                    ArrowBuffer.Empty);
            }
        }

        public Int64DictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
        }

        public Int64DictionaryArray(int length, int uniqueValues, ArrowBuffer indices, ArrowBuffer dataBuffer, ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0) :
            this(new ArrayData(DictionaryType.Default(ArrowTypeId.Int64), length, nullCount, offset, new[] { nullBitmapBuffer, indices, dataBuffer }), uniqueValues)
        {
        }
    }
}
