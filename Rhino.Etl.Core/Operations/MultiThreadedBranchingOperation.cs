using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows)
        {
            return new AsyncEnumerable<Row>(async yield => {
                var input = new GatedThreadSafeEnumerator<Row>(Operations.Count, rows);

                var sync = new SemaphoreSlim(1, 1);

                foreach (var operation in Operations)
                {
                    var clone = input.Select(r => r.Clone());
                    var result = operation.Execute(clone);

                    if (result == null)
                    {
                        await input.DisposeAsync();
                        continue;
                    }

                    var enumerator = result.GetAsyncEnumerator();

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
                                                         });
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