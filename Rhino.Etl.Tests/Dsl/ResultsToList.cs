using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class ResultsToList : AbstractProcessingOperation
    {
        public List<Row> Results = new List<Row>();

        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield yield)
        {
            Results.Add(row);

            await yield.ReturnAsync(row);
        }
    }
}