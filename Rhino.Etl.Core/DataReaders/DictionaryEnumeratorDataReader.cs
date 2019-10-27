using System.Threading;

namespace Rhino.Etl.Core.DataReaders
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A datareader over a collection of dictionaries
    /// </summary>
    public class DictionaryEnumeratorDataReader : EnumerableDataReader<Row>
    {
        private readonly IAsyncEnumerable<Row> enumerable;
        private readonly List<Descriptor> propertyDescriptors = new List<Descriptor>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryEnumeratorDataReader"/> class.
        /// </summary>
        /// <param name="schema">The schema.</param>
        /// <param name="enumerable">The enumerator.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
        public DictionaryEnumeratorDataReader(IDictionary<string, Type> schema,
                                              IAsyncEnumerable<Row>     enumerable,
                                              CancellationToken         cancellationToken = default)
            : base(enumerable.GetAsyncEnumerator(cancellationToken))
        {
            this.enumerable = enumerable;
            foreach (KeyValuePair<string, Type> pair in schema)
            {
                propertyDescriptors.Add(new DictionaryDescriptorAdapter(pair));
            }
        }

        /// <summary>
        /// Gets the descriptors for the properties that this instance
        /// is going to handle
        /// </summary>
        /// <value>The property descriptors.</value>
        protected override IList<Descriptor> PropertyDescriptors
        {
            get { return propertyDescriptors; }
        }

        /// <summary>
        /// Perform the actual closing of the reader
        /// </summary>
        protected override void DoClose()
        {
            enumerator.DisposeAsync();

            IDisposable disposable = enumerable as IDisposable;
            if (disposable != null)
                disposable.Dispose();
            IAsyncDisposable disposableAsync = enumerable as IAsyncDisposable;
            if (disposableAsync != null)
                enumerator.DisposeAsync();
        }
    }
}