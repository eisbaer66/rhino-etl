using System.Threading.Tasks;
using Nito.AsyncEx;

namespace Rhino.Etl.Core.Enumerables
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// This enumerator allows to safely move items between threads. It takes
    /// care of all the syncronization.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ThreadSafeEnumerator<T> : IAsyncEnumerable<T>, IAsyncEnumerator<T>
    {
        private bool active = true;
        private readonly AsyncMonitor monitor = new AsyncMonitor();
        private readonly Queue<T> cached = new Queue<T>();
        private T current;

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
        {
            return this;
        }

        /// <summary>
        /// Gets the element in the collection at the current position of the enumerator.
        /// </summary>
        /// <value></value>
        /// <returns>The element in the collection at the current position of the enumerator.</returns>
        public T Current
        {
            get { return current; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            cached.Clear();

            return new ValueTask();
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception>
        public async ValueTask<bool> MoveNextAsync()
        {
            using (await monitor.EnterAsync())
            {
                while (cached.Count == 0 && active)
                    await monitor.WaitAsync();

                if (active == false && cached.Count == 0)
                    return false;

                current = cached.Dequeue();

                return true;
            }
        }

        /// <summary>
        /// Adds the item to the items this is enumerating on.
        /// Will immediately release a waiting thread that can start working on itl
        /// </summary>
        /// <param name="item">The item.</param>
        public async Task AddItem(T item)
        {
            using (await monitor.EnterAsync())
            {
                cached.Enqueue(item);
                monitor.Pulse();
            }
        }

        /// <summary>
        /// Marks this instance as finished, so it will stop iterating
        /// </summary>
        public async Task MarkAsFinished()
        {
            using (await monitor.EnterAsync())
            {
                active = false;
                monitor.Pulse();
            }

        }
    }
}