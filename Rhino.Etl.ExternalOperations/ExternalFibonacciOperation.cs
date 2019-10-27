using System;
using System.Collections.Generic;
using Dasync.Collections;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace Rhino.Etl.ExternalOperations
{
    public class ExternalFibonacciOperation : AbstractOperation
    {
        private readonly int max;

        public ExternalFibonacciOperation(int max)
        {
            this.max = max;
        }

        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows)
        {
            return new AsyncEnumerable<Row>(async yield => {
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
            });
        }
    }
}
