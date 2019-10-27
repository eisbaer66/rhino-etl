using System.Threading;

namespace Rhino.Etl.Core.Pipelines
{
    using System.Collections.Generic;
    using Enumerables;
    using Operations;

    /// <summary>
    /// Executes the pipeline on a single thread
    /// </summary>
    public class SingleThreadedPipelineExecuter : AbstractPipelineExecuter
    {
        /// <summary>
        /// Add a decorator to the enumerable for additional processing
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="enumerator">The enumerator.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
        protected override IAsyncEnumerable<Row> DecorateEnumerableForExecution(
            IOperation            operation,
            IAsyncEnumerable<Row> enumerator,
            CancellationToken cancellationToken = default)
        {
            return new CachingEnumerable<Row>(new EventRaisingEnumerator(operation, enumerator), cancellationToken);
        }
    }
}