using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// Represent a single operation that can occure during the ETL process.
    /// Provides yield object for AsyncEnumerable&lt;Row&gt;
    /// </summary>
    public abstract class AbstractYieldOperation : AbstractOperation
    {
        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows, CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerable<Row>(async yield => { await ExecuteYield(rows, yield, cancellationToken); });
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">rows</param>
        /// <param name="yield">yield object for AsyncEnumerable&lt;Row&gt;</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        protected abstract Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield,
            CancellationToken cancellationToken = default);
    }
}