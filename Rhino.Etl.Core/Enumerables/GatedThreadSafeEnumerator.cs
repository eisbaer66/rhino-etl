using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Core.Enumerables
{
    /// <summary>
    /// An iterator to be consumed by concurrent threads only which supplies an element of the decorated enumerable one by one
    /// </summary>
    /// <typeparam name="T">The type of the decorated enumerable</typeparam>
    public class GatedThreadSafeEnumerator<T> :    WithLoggingMixin, IAsyncEnumerable<T>, IAsyncEnumerator<T>
    {
        private readonly int numberOfConsumers;
        private readonly IAsyncEnumerator<T> innerEnumerator;
        private int callsToMoveNext;
        private readonly SemaphoreSlim sync = new SemaphoreSlim(1,1);
        private bool moveNext;
        private T current;
        private int consumersLeft;

        /// <summary>
        /// Creates a new instance of <see cref="GatedThreadSafeEnumerator{T}"/>
        /// </summary>
        /// <param name="numberOfConsumers">The number of consumers that will be consuming this iterator concurrently</param>
        /// <param name="source">The decorated enumerable that will be iterated and fed one element at a time to all consumers</param>
        public GatedThreadSafeEnumerator(int numberOfConsumers, IAsyncEnumerable<T> source)
        {
            this.numberOfConsumers = numberOfConsumers;
            consumersLeft = numberOfConsumers;
            innerEnumerator = source.GetAsyncEnumerator();
        }

        ///    <summary>
        ///    Get    the    enumerator
        ///    </summary>
        ///    <returns></returns>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return this;
        }

        ///    <summary>
        ///    Dispose    the    enumerator
        ///    </summary>
        public async ValueTask DisposeAsync()
        {
            if(Interlocked.Decrement(ref consumersLeft) == 0)
            {
                Debug("Disposing inner enumerator");
                await innerEnumerator.DisposeAsync();
            }
        }

        ///    <summary>
        ///    MoveNext the enumerator
        ///    </summary>
        ///    <returns></returns>
        public async ValueTask<bool> MoveNextAsync()
        {
            await sync.Execute(async () =>
            {
                if (Interlocked.Increment(ref callsToMoveNext) == numberOfConsumers)
                {
                    callsToMoveNext = 0;
                    moveNext = await innerEnumerator.MoveNextAsync();
                    current = innerEnumerator.Current;

                    Debug("Pulsing all waiting threads");

                    Monitor.PulseAll(sync);
                }
                else
                {
                    Monitor.Wait(sync);
                }
            });
            return moveNext;
        }

        ///    <summary>
        ///    Reset the enumerator
        ///    </summary>
        public void Reset()
        {
            throw new NotSupportedException();
        }

        ///    <summary>
        ///    The    current    value of the enumerator
        ///    </summary>
        public T Current
        {
            get { return current; }
        }

        ///    <summary>
        ///    Number of consumers    left that have not yet completed
        ///    </summary>
        public int ConsumersLeft { get { return consumersLeft; } }
    }
}