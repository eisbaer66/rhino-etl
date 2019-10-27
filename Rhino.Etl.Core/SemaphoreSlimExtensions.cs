using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Core
{
    internal static class SemaphoreSlimExtensions
    {
        public static async Task Execute(this SemaphoreSlim semaphoreSlim, 
            Func<Task> func,
            CancellationToken cancellationToken = default)
        {
            await semaphoreSlim.WaitAsync(cancellationToken);

            try
            {
                await func();
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
        public static async Task Execute<T>(this SemaphoreSlim semaphoreSlim, 
            Func<Task<T>> func,
            CancellationToken cancellationToken = default)
        {
            await semaphoreSlim.WaitAsync(cancellationToken);

            try
            {
                await func();
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
    }
}