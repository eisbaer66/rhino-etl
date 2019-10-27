using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.LoadTest
{
    using System;
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class GenerateRandomIds : AbstractYieldOperation
    {
        public GenerateRandomIds(int expectedCount)
        {
            this.expectedCount = expectedCount;
        }

        private readonly int expectedCount;

        protected override async Task ExecuteYield(IAsyncEnumerable<Row>      rows,
                                                   AsyncEnumerator<Row>.Yield yield,
                                                   CancellationToken          cancellationToken = default)
        {
            for (int i = 0; i < expectedCount; i++)
            {
                Row row = new Row();
                row["old_id"] = i;
                row["new_id"] = Guid.NewGuid();
                await @yield.ReturnAsync(row);
            }
        }
    }
}