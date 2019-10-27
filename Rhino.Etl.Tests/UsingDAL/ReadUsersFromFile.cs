using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Files;
    using Rhino.Etl.Core.Operations;

    public class ReadUsersFromFile : AbstractYieldOperation
    {
        protected override async Task ExecuteYield(IAsyncEnumerable<Row>      rows,
                                                   AsyncEnumerator<Row>.Yield yield,
                                                   CancellationToken          cancellationToken = default)
        {
            using(FileEngine file = FluentFile.For<UserRecord>().From("users.txt"))
            {
                foreach (object obj in file)
                {
                    await @yield.ReturnAsync(Row.FromObject(obj));
                }
            }
        }
    }
}