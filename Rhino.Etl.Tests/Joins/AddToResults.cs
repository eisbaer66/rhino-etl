using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Joins
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class AddToResults : AbstractYieldOperation
    {
        private readonly ICollection<Row> results;

        public AddToResults(ICollection<Row> results)
        {
            this.results = results;
        }

        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield @yield)
        {
            await rows.ForEachAsync(row => { results.Add(row); });
        }
    }
}