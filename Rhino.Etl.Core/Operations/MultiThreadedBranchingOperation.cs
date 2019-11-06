using System.Collections.Generic;
using System.Threading;
using Dasync.Collections;
using Nito.AsyncEx;
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

                AsyncMonitor monitor = new AsyncMonitor();

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
                                                         using (await monitor.EnterAsync(cancellationToken))
                                                         {
                                                             await enumerator.DisposeAsync();
                                                             monitor.Pulse();
                                                         }
                                                     }
                                                 });
                }

                using (await monitor.EnterAsync(cancellationToken))
                    while (input.ConsumersLeft > 0)
                        await monitor.WaitAsync(cancellationToken);

                yield.Break();
            });
        }
    }
}