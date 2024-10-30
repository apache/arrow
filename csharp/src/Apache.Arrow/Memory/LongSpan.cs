using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal readonly struct LongSpan<T>
    {
        private readonly LongMemory<T> _memory;

        public LongSpan(LongMemory<T> memory)
        {
            _memory = memory;
        }

        public long Length
        {
            get { return _memory.Length; }
        }

        public T this[long index]
        {
            get
            {
                if (index < 0 || index >= Length)
                {
                    throw new IndexOutOfRangeException("Index is out of range.");
                }
                return _memory[index];
            }
        }

        public Span<T> AsSpan()
        {
            return _memory.Span;
        }
    }
}
