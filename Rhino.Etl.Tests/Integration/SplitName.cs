using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Integration
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class SplitName : AbstractProcessingOperation
    {
        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield)
        {
            string name = (string)row["name"];
            row["FirstName"] = name.Split()[0];
            row["LastName"] = name.Split()[1];
            await @yield.ReturnAsync(row);
        }
    }
}