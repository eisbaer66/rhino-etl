using System.Collections.Generic;
using System.Threading.Tasks;
using Dasync.Collections;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Rhino.Etl.Tests.Joins
{
    public class GenericEnumerableOperation : AbstractYieldOperation
    {
        private readonly IEnumerable<Row> rowsToReturn;

        public GenericEnumerableOperation(IEnumerable<Row> rows)
        {
            rowsToReturn = rows;
        }

        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield @yield)
        {
            foreach (Row row in rowsToReturn)
            {
                await yield.ReturnAsync(row);
            }
        }
    }
}