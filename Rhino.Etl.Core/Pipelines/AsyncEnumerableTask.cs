using System.Collections.Generic;
using System.Threading.Tasks;
using Rhino.Etl.Core.Enumerables;

namespace Rhino.Etl.Core.Pipelines
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AsyncEnumerableTask<T>
    {
        /// <summary>
        /// 
        /// </summary>
        public IAsyncEnumerable<T> Enumerable { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public Task Task { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="enumerable"></param>
        /// <returns></returns>
        public static AsyncEnumerableTask<T> Completed(IAsyncEnumerable<T> enumerable)
        {
            return new AsyncEnumerableTask<T>
                   {
                       Task       = Task.CompletedTask,
                       Enumerable = enumerable,
                   };
        }
    }
}