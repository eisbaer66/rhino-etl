using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.LoadTest
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class UpperCaseColumn : AbstractProcessingOperation
    {
        private readonly string column;

        public UpperCaseColumn(string column)
        {
            this.column = column;
        }
        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield)
        {
                row[column] = ((string) row[column] ?? "").ToUpper();
                row["testMsg"] = "UpperCased";
                await yield.ReturnAsync(row);
        }
    }
}