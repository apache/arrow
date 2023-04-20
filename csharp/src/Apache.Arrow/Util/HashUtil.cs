using System;
using System.Linq;

namespace Apache.Arrow.Util
{
    public class HashUtil
    {
#if NETCOREAPP3_1_OR_GREATER
        public static int Hash32(dynamic o) => Tuple.Create(o).GetHashCode();
        public static int Hash32(dynamic o1, dynamic o2) => Tuple.Create(o1, o2).GetHashCode();
        public static int Hash32(dynamic o1, dynamic o2, dynamic o3) => Tuple.Create(o1, o2, o3).GetHashCode();
        public static int Hash32(dynamic o1, dynamic o2, dynamic o3, dynamic o4) => Tuple.Create(o1, o2, o3, o4).GetHashCode();
#else
        public static int Hash32(object o) => Tuple.Create(o).GetHashCode();
        public static int Hash32(object o1, object o2) => Tuple.Create(o1, o2).GetHashCode();
        public static int Hash32(object o1, object o2, object o3) => Tuple.Create(o1, o2, o3).GetHashCode();
        public static int Hash32(object o1, object o2, object o3, object o4) => Tuple.Create(o1, o2, o3, o4).GetHashCode();
#endif
        public static int Hash32Array(int[] array)
        {
            int length = array.Length;

            switch (length)
            {
                case 0:
                    throw new ArgumentException("Cannot GetHashCode with 0 parameters");
                case 1:
                    return Tuple.Create(array[0]).GetHashCode();
                case 2:
                    return Tuple.Create(array[0], array[1]).GetHashCode();
                case 3:
                    return Tuple.Create(array[0], array[1], array[2]).GetHashCode();
                case 4:
                    return Tuple.Create(array[0], array[1], array[2], array[3]).GetHashCode();
                case 5:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4]).GetHashCode();
                case 6:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5]).GetHashCode();
                case 7:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5], array[6]).GetHashCode();
                case 8:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7]).GetHashCode();
                default:
                    Type tupleType = typeof(Tuple<,,,,>).MakeGenericType(array.Cast<object>().Select(x => x.GetType()).ToArray());
                    return Activator.CreateInstance(tupleType, array.Cast<object>().ToArray()).GetHashCode();
            }
        }
    }
}
