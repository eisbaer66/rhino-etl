using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Operations
{
    using System.Collections.Generic;

    /// <summary>
    /// An aggregation operation, handles all the backend stuff of the aggregation,
    /// leaving client code just the accumulation process
    /// </summary>
    public abstract class AbstractAggregationOperation : AbstractOperation
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
                IDictionary<ObjectArrayKeys, Row> aggregations = new Dictionary<ObjectArrayKeys, Row>();
                string[] groupBy = GetColumnsToGroupBy();
                await rows.ForEachAsync(row =>
                {
                    ObjectArrayKeys key = row.CreateKey(groupBy);
                    Row aggregate;
                    if (aggregations.TryGetValue(key, out aggregate) == false)
                        aggregations[key] = aggregate = new Row();
                    Accumulate(row, aggregate);

                    return Task.CompletedTask;
                }, cancellationToken);
                foreach (Row row in aggregations.Values)
                {
                    FinishAggregation(row);
                    await yield.ReturnAsync(row);
                }
            });
        }

        /// <summary>
        /// Allow a derived class to perform final processing on the
        /// aggregate, before sending it downward in the pipeline.
        /// </summary>
        /// <param name="aggregate">The row.</param>
        protected virtual async void FinishAggregation(Row aggregate)
        {
            await Task.CompletedTask;
        }

        /// <summary>
        /// Accumulate the current row to the current aggregation
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="aggregate">The aggregate.</param>
        protected abstract void Accumulate(Row row, Row aggregate);

        /// <summary>
        /// Gets the columns list to group each row by
        /// </summary>
        /// <value>The group by.</value>
        protected virtual string[] GetColumnsToGroupBy()
        {
            return new string[0];
        }
    }
}