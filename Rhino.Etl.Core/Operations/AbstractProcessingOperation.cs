using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// Represent a single operation that can occure during the ETL process.
    /// Processes rows asynchronously
    /// </summary>
    public abstract class AbstractProcessingOperation : AbstractYieldOperation
    {
        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">rows</param>
        /// <param name="yield">yield object for AsyncEnumerable&lt;Row&gt;</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield,
            CancellationToken cancellationToken = default)
        {
            ; await rows.ForEachAsync(r => ExecuteAsync(r, yield), cancellationToken);
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="row">current row</param>
        /// <param name="yield">yield object for AsyncEnumerable&lt;Row&gt;</param>
        /// <returns></returns>
        protected abstract Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield);
    }
}