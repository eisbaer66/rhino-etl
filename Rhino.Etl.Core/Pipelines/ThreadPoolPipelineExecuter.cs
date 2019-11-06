using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Pipelines
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Enumerables;
    using Operations;

    /// <summary>
    /// Execute all the actions concurrently, in the thread pool
    /// </summary>
    public class ThreadPoolPipelineExecuter : AbstractPipelineExecuter
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
            CancellationToken     cancellationToken = default)
        {
            ThreadSafeEnumerator<Row> threadedEnumerator = new ThreadSafeEnumerator<Row>();
            ThreadPool.QueueUserWorkItem(async delegate
            {
                try
                {
                    IAsyncEnumerable<Row> eventRaisingEnumerator = new EventRaisingEnumerator(operation, enumerator);
                    await eventRaisingEnumerator
                        .ForEachAsync(async t => { await threadedEnumerator.AddItem(t); },
                                      cancellationToken: cancellationToken);
                }
                catch (Exception e)
                {
                    Error(e, "Failed to execute operation {0}", new Tuple<string, object>("Operation", operation));
                    await threadedEnumerator.MarkAsFinished();
#if DEBUG
                    throw;
#endif
                }
                finally
                {
                    await threadedEnumerator.MarkAsFinished();
                }
            });
            return threadedEnumerator;
        }
    }
}