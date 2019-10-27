using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Errors
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Core;
    using Core.Operations;

    public class ThrowingOperation : AbstractYieldOperation
    {
        private readonly int rowsAfterWhichToThrow = new Random().Next(1, 6);

        public int RowsAfterWhichToThrow
        {
            get { return rowsAfterWhichToThrow; }
        }

        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield)
        {
            for (int i = 0; i < RowsAfterWhichToThrow; i++)
            {
                Row row = new Row();
                row["id"] = i;
                await yield.ReturnAsync(row);
            }
            throw new InvalidDataException("problem");
        }
    }
}
