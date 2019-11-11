using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
            return new AsyncEnumerable<Row>(yield => {
                var input = new GatedThreadSafeEnumerator<Row>(Operations.Count, rows, cancellationToken);

                AsyncMonitor monitor = new AsyncMonitor();

                Task[] tasks = Operations
                            .Select(async operation => 
                                    {
                                        var clone  = input.Select(r => r.Clone());
                                        var result = operation.Execute(clone, cancellationToken);

                                        if (result == null)
                                        {
                                            await input.DisposeAsync();
                                            return null;
                                        }

                                        var enumerator = result.GetAsyncEnumerator(cancellationToken);

                                        return Task.Run(async () =>
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
                                                        }, cancellationToken);
                                    })
                            .Where(t => t?.Result != null)
                            .Select(t => t.Result)
                            .ToArray();

                Task.WaitAll(tasks);

                return Task.CompletedTask;
            });
        }
    }
}