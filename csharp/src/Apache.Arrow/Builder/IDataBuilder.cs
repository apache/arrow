using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public interface IDataBuilder
    {
        IArrowType DataType { get; }
        int Length { get; }
        int NullCount { get; }
        int Offset { get; }

        IBufferBuilder[] Buffers { get; }
        IDataBuilder[] Children { get; }
        IDataBuilder Dictionary { get; } // Only used for dictionary type

        ArrayData FinishInternal(MemoryAllocator allocator = default);
    }

    public interface IArrayBuilder : IDataBuilder
    {
        IArrayBuilder AppendNull();
        IArrayBuilder AppendNulls(int count);

        IArrowArray Build(MemoryAllocator allocator = default);
    }
}
