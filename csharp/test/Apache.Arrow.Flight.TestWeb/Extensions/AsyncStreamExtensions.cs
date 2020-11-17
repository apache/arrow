using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Grpc.Core.Utils
{
    public static class AsyncStreamExtensions
    {
        /// <summary>
        /// Reads the entire stream and creates a list containing all the elements read.
        /// </summary>
        public static async Task<List<T>> ToListAsync<T>(this IAsyncStreamReader<T> streamReader)
            where T : class
        {
            var result = new List<T>();
            while (await streamReader.MoveNext().ConfigureAwait(false))
            {
                result.Add(streamReader.Current);
            }
            return result;
        }
    }
}
