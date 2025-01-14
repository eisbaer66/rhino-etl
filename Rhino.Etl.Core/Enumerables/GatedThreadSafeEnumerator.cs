﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

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
        private readonly AsyncMonitor monitor = new AsyncMonitor();
        private bool moveNext;
        private T current;
        private int consumersLeft;

        /// <summary>
        /// Creates a new instance of <see cref="GatedThreadSafeEnumerator{T}"/>
        /// </summary>
        /// <param name="numberOfConsumers">The number of consumers that will be consuming this iterator concurrently</param>
        /// <param name="source">The decorated enumerable that will be iterated and fed one element at a time to all consumers</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
        public GatedThreadSafeEnumerator(int                 numberOfConsumers,
                                         IAsyncEnumerable<T> source,
                                         CancellationToken   cancellationToken = default)
        {
            this.numberOfConsumers = numberOfConsumers;
            consumersLeft = numberOfConsumers;
            innerEnumerator = source.GetAsyncEnumerator(cancellationToken);
        }

        ///    <summary>
        ///    Get    the    enumerator
        ///    </summary>
        ///    <returns></returns>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
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
            using(await monitor.EnterAsync())
            {
                if (Interlocked.Increment(ref callsToMoveNext) == numberOfConsumers)
                {
                    callsToMoveNext = 0;
                    moveNext = await innerEnumerator.MoveNextAsync();
                    current = innerEnumerator.Current;

                    Debug("Pulsing all waiting threads");

                    monitor.PulseAll();
                }
                else
                {
                    await monitor.WaitAsync();
                }
            }
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