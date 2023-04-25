using System;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class BaseArrayBuilder : IArrayBuilder
    {
        public IArrowType DataType { get; }

        public int Length { get; private set; }

        public int NullCount { get; private set; }

        public int Offset { get; }

        public IBufferBuilder[] Buffers { get; }
        public IBufferBuilder ValidityBuffer => Buffers[0];

        public IDataBuilder[] Children { get; }

        public IDataBuilder Dictionary { get; }

        public BaseArrayBuilder(
            IArrowType dataType,
            IBufferBuilder[] buffers,
            IDataBuilder[] children = null,
            IDataBuilder dictionary = null
        )
        {
            DataType = dataType;

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
            bool[] values = new bool[count]; // create a new bool array of length x

            for (int i = 0; i < count; i++)
            {
                values[i] = true; // set each value in the array to true
            }

            ValidityBuffer.AppendBits(new ReadOnlySpan<bool>(values));
            NullCount++;
            return this;
        }

        public virtual IArrayBuilder AppendValue()
        {
            Length++;
            return this;
        }

        public virtual IArrayBuilder AppendValues(int count)
        {
            Length += count;
            return this;
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
