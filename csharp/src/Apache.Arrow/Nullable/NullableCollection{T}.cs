namespace Apache.Arrow.Nullable
{
    using System.Collections.Generic;

    public class NullableCollection<T>
    {
        public NullableCollection(int count)
        {
            this.Count = count;
            this.Values = new List<T>(count);
            this.Nulls = new ValidityBitmap(count);
        }

        public int Count { get; }

        public List<T> Values { get; }

        public ValidityBitmap Nulls { get; }
    }
}