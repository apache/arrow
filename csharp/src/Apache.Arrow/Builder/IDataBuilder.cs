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

        IValueBufferBuilder[] Buffers { get; }
        IDataBuilder[] Children { get; }
        IDataBuilder Dictionary { get; } // Only used for dictionary type

        IArrayBuilder AppendNull();
        IArrayBuilder AppendNulls(int count);
        IArrayBuilder AppendValues(ArrayData data);

        ArrayData FinishInternal(MemoryAllocator allocator = default);
    }

    public interface IArrayBuilder : IDataBuilder
    {
        IArrowArray Build(MemoryAllocator allocator = default);
    }
}
