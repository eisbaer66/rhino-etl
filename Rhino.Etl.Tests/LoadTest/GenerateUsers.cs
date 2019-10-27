using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.LoadTest
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class GenerateUsers : AbstractYieldOperation
    {
        public GenerateUsers(int expectedCount)
        {
            this.expectedCount = expectedCount;
        }

        private int expectedCount;

        protected override async Task ExecuteYield(IAsyncEnumerable<Row>      rows,
                                                   AsyncEnumerator<Row>.Yield yield,
                                                   CancellationToken          cancellationToken = default)
        {
            for (int i = 0; i < expectedCount; i++)
            {
                Row row = new Row();
                row["id"] = i;
                row["name"] = "ayende #" + i;
                row["email"] = "ayende" + i + "@example.org";
                await @yield.ReturnAsync(row);
            }
        }
    }
}