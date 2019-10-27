using System.Collections.Generic;
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
        protected override IAsyncEnumerable<Row> DecorateEnumerableForExecution(IOperation operation, IAsyncEnumerable<Row> enumerator)
        {
            return new AsyncEnumerable<Row>(async yield => {
                foreach (Row row in new EventRaisingEnumerator(operation, enumerator))
                {
                    await yield.ReturnAsync(row);
                }
            });
        }
    }
}