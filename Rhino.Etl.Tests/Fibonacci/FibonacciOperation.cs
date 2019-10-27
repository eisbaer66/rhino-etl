using System.Collections.Generic;
using System.Threading.Tasks;
using Dasync.Collections;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Rhino.Etl.Tests.Fibonacci
{
    public class FibonacciOperation : AbstractYieldOperation
    {
        private readonly int max;

        public FibonacciOperation(int max)
        {
            this.max = max;
        }

        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield)
        {
            int a = 0;
            int b = 1;
            var row = new Row();
            row["id"] = 1;
            await yield.ReturnAsync(row);

            for (int i = 0; i < max - 1; i++)
            {
                int c = a + b;
                row = new Row();
                row["id"] = c;
                await yield.ReturnAsync(row);

                a = b;
                b = c;
            }
        }
    }
}