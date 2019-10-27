using System.Collections.Generic;
using System.Threading;
using Dasync.Collections;
using Rhino.Etl.Core.Enumerables;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// Branch the current pipeline flow into all its inputs
    /// </summary>
    public class MultiThreadedBranchingOperation : AbstractBranchingOperation
    {
        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows, CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerable<Row>(async yield => {
                var input = new GatedThreadSafeEnumerator<Row>(Operations.Count, rows, cancellationToken);

                var sync = new SemaphoreSlim(1, 1);

                foreach (var operation in Operations)
                {
                    var clone = input.Select(r => r.Clone());
                    var result = operation.Execute(clone, cancellationToken);

                    if (result == null)
                    {
                        await input.DisposeAsync();
                        continue;
                    }

                    var enumerator = result.GetAsyncEnumerator(cancellationToken);

                    ThreadPool.QueueUserWorkItem(async delegate
                                                 {
                                                     try
                                                     {
                                                         while (await enumerator.MoveNextAsync()) ;
                                                     }
                                                     finally
                                                     {
                                                         await sync.Execute(async () =>
                                                         {
                                                             await enumerator.DisposeAsync();
                                                             Monitor.Pulse(sync);
                                                         }, cancellationToken);
                                                     }
                                                 });
                }

                lock (sync)
                    while (input.ConsumersLeft > 0)
                        Monitor.Wait(sync);

                yield.Break();
            });
        }
    }
}