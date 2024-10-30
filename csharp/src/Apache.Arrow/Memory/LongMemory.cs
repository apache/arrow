using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal readonly struct LongMemory<T>
    {
        private readonly T[] _array;
        private readonly long _length;

        public LongMemory(long length)
        {
            if (length < 0 || length > _array.Length) throw new ArgumentOutOfRangeException(nameof(length));
            _array = new T[length];
            _length = length;
        }

        public long Length
        {
            get { return _length; }
        }

        public T this[long index]
        {
            get
            {
                if (index < 0 || index >= _array.Length)
                {
                    throw new IndexOutOfRangeException("Index is out of range.");
                }
                return _array[index];
            }

            set
            {
                if (index < 0 || index >= _length)
                    throw new IndexOutOfRangeException("Index is out of range.");
                _array[index] = value;
            }
        }

        public void CopyTo(LongMemory<T> destination)
        {
            if (destination.Length < _length)
            {
                throw new ArgumentException("Destination memory is not long enough.", nameof(destination));
            }

            for (long i = 0; i < _length; i++)
            {
                destination[i] = _array[i];
            }
        }

        public Span<T> Span
        {
            get { return _array.AsSpan(0, (int)_length); }
        }   
        public void Clear()
        {
            for (long i = 0; i < _length; i++)
            {
                _array[i] = default!;
            }
        }
    }
}
