namespace Apache.Arrow
{
    public interface IRecord
    {
        /// <summary>
        /// Gets the value type at index.
        /// </summary>
        /// <value></value>
        System.Type PropertyType(int index);

        /// <summary>
        /// Gets the value of the item at the specified index.
        /// </summary>
        /// <value></value>
        object this[int index] { get; }
    }
}
