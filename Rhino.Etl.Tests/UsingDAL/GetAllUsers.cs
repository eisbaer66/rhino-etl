using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class GetAllUsers : AbstractYieldOperation
    {
        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield,
            CancellationToken cancellationToken = default)
        {
            foreach (User user in MySimpleDal.GetUsers())
            {
                await @yield.ReturnAsync(Row.FromObject(user));
            }
        }
    }
}