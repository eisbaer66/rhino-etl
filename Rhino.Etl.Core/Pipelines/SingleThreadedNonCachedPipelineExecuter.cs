using System.Collections.Generic;
using System.Threading;
using Dasync.Collections;
using Rhino.Etl.Core.Enumerables;
using Rhino.Etl.Core.Operations;

namespace Rhino.Etl.Core.Pipelines
{
    /// <summary>
    /// Execute all the actions syncronously without caching
    /// </summary>
    public class SingleThreadedNonCachedPipelineExecuter : AbstractPipelineExecuter
    {
        /// <summary>
        /// Add a decorator to the enumerable for additional processing
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="enumerator">The enumerator.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
        protected override IAsyncEnumerable<Row> DecorateEnumerableForExecution(IOperation            operation,
                                                                                IAsyncEnumerable<Row> enumerator,
                                                                                CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerable<Row>(async yield =>
                                            {
                                                await new EventRaisingEnumerator(operation, enumerator)
                                                    .ForEachAsync(async row => { await yield.ReturnAsync(row); });
                                            });
        }
    }
}