using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using Core;
    using FileHelpers;
    using Rhino.Etl.Core.Files;
    using Rhino.Etl.Core.Operations;

    public class WriteUsersToFile : AbstractYieldOperation
    {
        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield yield,
            CancellationToken cancellationToken = default)
        {
            FluentFile engine = FluentFile.For<UserRecord>();
            engine.HeaderText = "Id\tName\tEmail";
            using(FileEngine file = engine.To("users.txt"))
            {

                await rows.ForEachAsync(row =>
                {
                    UserRecord record = new UserRecord();

                    record.Id = (int) row["id"];
                    record.Name = (string) row["name"];
                    record.Email = (string) row["email"];

                    file.Write(record);
                }, cancellationToken);
            }
            @yield.Break();
        }
    }
}