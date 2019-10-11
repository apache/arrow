using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays.DictionaryArrays
{
    public class Date32DictionaryArray : PrimitiveDictionaryArray<int>
    {
        private const int MillisecondsPerDay = 86400000;

        public class Builder : PrimitiveDictionaryArrayBuilder<DateTimeOffset, int, Date32DictionaryArray, Builder>
        {
            public Builder() : base(new DateBuilder()) { }

            internal class DateBuilder : PrimitiveDictionaryArrayBuilder<int, Date32DictionaryArray, DateBuilder>
            {

                public DateBuilder(IEqualityComparer<int> comparer = null, HashFunctionDelegate hashFunc = null) : base(
                    comparer, hashFunc)
                {
                }

                /// <inheritdoc />
                public override Date32DictionaryArray Build(MemoryAllocator allocator)
                {
                    allocator = allocator ?? MemoryAllocator.Default.Value;

                    return new Date32DictionaryArray(IndicesBuffer.Length, ValuesBuffer.Length, IndicesBuffer.Build(allocator), ValuesBuffer.Build(allocator),
                        ArrowBuffer.Empty);
                }
            }

            /// <inheritdoc />
            protected override int ConvertTo(DateTimeOffset value)
            {
                return (int)(value.ToUnixTimeMilliseconds() / MillisecondsPerDay);

            }

            /// <inheritdoc />
            public Builder(IEqualityComparer<int> comparer = null, HashFunctionDelegate hashFunc = null) : base(new DateBuilder(comparer, hashFunc))
            {
            }
        }

        public Date32DictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
        }

        public Date32DictionaryArray(int length, int uniqueValues, ArrowBuffer indices, ArrowBuffer dataBuffer, ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0) :
            this(new ArrayData(DictionaryType.Default(ArrowTypeId.Date32), length, nullCount, offset, new[] { nullBitmapBuffer, indices, dataBuffer }), uniqueValues)
        {
        }
    }
}
