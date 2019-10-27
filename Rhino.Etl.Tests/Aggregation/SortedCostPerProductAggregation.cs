﻿using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Aggregation
{
    using Core;
    using Rhino.Etl.Core.Operations;

    public class SortedCostPerProductAggregation : AbstractSortedAggregationOperation
    {
        /// <summary>
        /// Accumulate the current row to the current aggregation
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="aggregate">The aggregate.</param>
        protected override Task Accumulate(Row row, Row aggregate)
        {
            aggregate["name"] = row["name"];
            if (aggregate["cost"] == null)
                aggregate["cost"] = 0;
            aggregate["cost"] = ((int)aggregate["cost"]) + ((int)row["price"]);

            return Task.CompletedTask;
        }

        protected override string[] GetColumnsToGroupBy()
        {
            return new string[] { "name" };
        }
    }
}