using Dasync.Collections;

namespace Rhino.Etl.Core.Operations
{
    using System.Collections.Generic;

    /// <summary>
    /// An aggregation operation, handles all the backend stuff of the aggregation,
    /// leaving client code just the accumulation process. Assumes a sorted rowset
    /// so that we can return early instead of having to accumulate all rows.
    /// </summary>
    public abstract class AbstractSortedAggregationOperation : AbstractAggregationOperation
    {
        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The pre-sorted rows.</param>
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows)
        {
            return new AsyncEnumerable<Row>(async yield => {
                ObjectArrayKeys previousKey = null;
                var aggregate = new Row();
                var groupBy = GetColumnsToGroupBy();

                await rows.ForEachAsync(async row =>
                {
                    var key = row.CreateKey(groupBy);

                    if (previousKey != null && !previousKey.Equals(key))
                    {
                        await FinishAggregation(aggregate);
                        await yield.ReturnAsync(aggregate);
                        aggregate = new Row();
                    }

                    await Accumulate(row, aggregate);
                    previousKey = key;
                });

                await FinishAggregation(aggregate);
                await yield.ReturnAsync(aggregate);
            });
        }
    }
}