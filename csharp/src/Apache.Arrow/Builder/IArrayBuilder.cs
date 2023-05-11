using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public interface IArrayBuilder
    {
        IArrowType DataType { get; }
        int Length { get; }
        int NullCount { get; }
        int Offset { get; }

        IValueBufferBuilder[] Buffers { get; }
        IArrayBuilder[] Children { get; }
        IArrayBuilder Dictionary { get; } // Only used for dictionary type

        /// <summary>
        /// Cast current builder to another one.
        /// </summary>
        /// <returns>Current casted</returns>
        TBuilder As<TBuilder>() where TBuilder : IArrayBuilder;

        /// <summary>
        /// Append a null value to builder.
        /// </summary>
        /// <returns>Current <see cref="Status"></returns>
        Status AppendNull();

        /// <summary>
        /// Append a number of null values to builder.
        /// </summary>
        /// <returns>Current <see cref="Status"></returns>
        Status AppendNulls(int count);

        /// <summary>
        /// Append <see cref="IScalar"> value.
        /// </summary>
        /// <returns>Current <see cref="Status"></returns>
        Status AppendScalar(IScalar value);

        /// <summary>
        /// Append <see cref="ArrayData"> values.
        /// </summary>
        /// <returns>Current <see cref="Status"></returns>
        Status AppendArray(ArrayData data);

        /// <summary>
        /// Append <see cref="IArrowArray"> values.
        /// </summary>
        /// <returns>Current <see cref="Status"></returns>
        Status AppendArray(IArrowArray data);

        /// <summary>
        /// Clear all contents appended so far.
        /// </summary>
        void Clear();

        /// <summary>
        /// Ensure that there is enough space allocated to append the indicated number of elements without
        /// any further reallocation.
        /// </summary>
        /// <remarks>
        /// Overallocation is used in order to minimize the impact of incremental Reserve() calls.
        /// 
        /// Note that additionnalCapacity is relative to the current number of elements rather than
        /// to the current capacity, so calls to Reserve() which are not interspersed with addition of new elements
        /// may not increase the capacity.
        /// </remarks>
        /// <param name="additionnalCapacity">number of additional array values</param>
        /// <returns>Current <see cref="Status"></returns>
        Status Reserve(int additionnalCapacity);

        /// <summary>
        /// Ensure that enough memory has been allocated to fit the indicated number of total elements in the builder,
        /// including any that have already been appended.
        /// </summary>
        /// <remarks>
        /// Does not account for reallocations that may be due to variable size data, like binary values.
        /// To make space for incremental appends, use Reserve instead.
        /// </remarks>
        /// <param name="capacity">minimum number of total array values to accommodate. Must be greater than the current capacity.</param>
        /// <returns>Current <see cref="Status"></returns>
        Status Resize(int capacity);

        /// <summary>
        /// Return result of builder as an internal generic ArrayData object.
        /// </summary>
        /// <remarks>
        /// Resets builder except for dictionary builder
        /// </remarks>
        /// <param name="allocator"><see cref="MemoryAllocator"/></param>
        /// <returns><see cref="ArrayData"/></returns>
        ArrayData FinishInternal(MemoryAllocator allocator = default);

        /// <summary>
        /// Return result of builder as an Array object.
        /// </summary>
        /// <remarks>
        /// Resets builder except for dictionary builder
        /// </remarks>
        /// <param name="allocator"><see cref="MemoryAllocator"/></param>
        /// <returns><see cref="IArrowArray"/></returns>
        IArrowArray Build(MemoryAllocator allocator = default);
    }
}
