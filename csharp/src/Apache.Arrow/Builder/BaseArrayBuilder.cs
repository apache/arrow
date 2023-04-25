using System;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class BaseArrayBuilder : IArrayBuilder
    {
        public IArrowType DataType { get; }

        public int Length { get; internal set; }

        public int NullCount { get; private set; }

        public int Offset { get; }

        public IBufferBuilder[] Buffers { get; }
        public IValueBufferBuilder<bool> ValidityBuffer { get; }

        public IDataBuilder[] Children { get; }

        public IDataBuilder Dictionary { get; }

        public BaseArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validityBuffer,
            IBufferBuilder[] buffers,
            IDataBuilder[] children = null,
            IDataBuilder dictionary = null
        )
        {
            DataType = dataType;

            ValidityBuffer = validityBuffer;
            Buffers = buffers;
            Children = children;
            Dictionary = dictionary;

            Length = 0;
            NullCount = 0;
            Offset = 0;
        }

        public virtual IArrayBuilder AppendNull()
        {
            ValidityBuffer.AppendBit(true);
            NullCount++;
            return this;
        }

        public virtual IArrayBuilder AppendNulls(int count)
        {
            ValidityBuffer.AppendBits(ValidityMask(count, false));
            NullCount += count;
            return this;
        }

        internal Span<bool> ValidityMask(int count, bool isValid)
        {
            Span<bool> values = new bool[count]; // create a new bool array of length count

            for (int i = 0; i < count; i++)
            {
                values[i] = isValid; // set each value in the array to isValid
            }

            return values;
        }

        public ArrayData FinishInternal(MemoryAllocator allocator = null)
            => new ArrayData(
                DataType, Length, NullCount, Offset,
                Buffers.Select(b => b.Build()).ToArray(),
                Children?.Select(c => c.FinishInternal()).ToArray(),
                Dictionary?.FinishInternal()
            );

        public virtual IArrowArray Build(MemoryAllocator allocator = null)
            => ArrowArrayFactory.BuildArray(FinishInternal(allocator));
    }
}
