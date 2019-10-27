using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Enumerables
{
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// There are several places where we need to iterate over an enumerable
    /// several times, but we cannot assume that it is safe to do so.
    /// This class will allow to safely use an enumerable multiple times, by caching
    /// the results after the first iteration.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CachingEnumerable<T> : IAsyncEnumerable<T>, IAsyncEnumerator<T>
    {
        private bool? isFirstTime = null;
        private IAsyncEnumerator<T> internalEnumerator;
        private readonly LinkedList<T> cache = new LinkedList<T>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CachingEnumerable&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="inner">The inner.</param>
        public CachingEnumerable(IAsyncEnumerable<T> inner)
        {
            internalEnumerator = inner.GetAsyncEnumerator();
        }

        ///<summary>
        ///Returns an enumerator that iterates through the collection.
        ///</summary>
        ///
        ///<returns>
        ///A <see cref="T:System.Collections.Generic.IEnumerator`1"></see> that can be used to iterate through the collection.
        ///</returns>
        ///<filterpriority>1</filterpriority>
        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            if(isFirstTime==null)
            {
                isFirstTime = true;
            }
            else if(isFirstTime.Value)
            {
                isFirstTime = false;
                internalEnumerator.DisposeAsync();
                internalEnumerator = cache.GetAsyncEnumerator();
            }
            else 
            {
                internalEnumerator = cache.GetAsyncEnumerator();
            }

            return this;
        }

        ///<summary>
        ///Returns an enumerator that iterates through a collection.
        ///</summary>
        ///
        ///<returns>
        ///An <see cref="T:System.Collections.IEnumerator"></see> object that can be used to iterate through the collection.
        ///</returns>
        ///<filterpriority>2</filterpriority>
        public IEnumerator GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }

        ///<summary>
        ///Gets the element in the collection at the current position of the enumerator.
        ///</summary>
        ///
        ///<returns>
        ///The element in the collection at the current position of the enumerator.
        ///</returns>
        ///
        T IAsyncEnumerator<T>.Current
        {
            get { return internalEnumerator.Current; }
        }

        ///<summary>
        ///Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        ///</summary>
        ///<filterpriority>2</filterpriority>
        public async ValueTask DisposeAsync()
        {
            await internalEnumerator.DisposeAsync();
        }

        ///<summary>
        ///Advances the enumerator to the next element of the collection.
        ///</summary>
        ///
        ///<returns>
        ///true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        ///</returns>
        ///
        ///<exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception><filterpriority>2</filterpriority>
        public async ValueTask<bool> MoveNextAsync()
        {
            bool result = await internalEnumerator.MoveNextAsync();
            if (result && isFirstTime.Value)
                cache.AddLast(internalEnumerator.Current);
            return result;
        }

        ///<summary>
        ///Gets the current element in the collection.
        ///</summary>
        ///
        ///<returns>
        ///The current element in the collection.
        ///</returns>
        ///
        ///<exception cref="T:System.InvalidOperationException">The enumerator is positioned before the first element of the collection or after the last element.-or- The collection was modified after the enumerator was created.</exception><filterpriority>2</filterpriority>
        public object Current
        {
            get { return internalEnumerator.Current; }
        }
    }
}