using System.Data.Common;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;
using Xunit;

namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Core;

    public class InputTimeoutFixture : BaseUserToPeopleTest
    {
        [Fact]
        public async Task CanCompile()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/InputTimeout.boo"))
                Assert.NotNull(process);
        }

        [Fact]
        public async Task CanCopyTableWithTimeout()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/InputTimeout.boo"))
                await process.Execute();
            
            List<string> names = await Use.Transaction<List<string>>("test", async delegate(DbCommand cmd)
            {
                List<string> tuples = new List<string>();
                cmd.CommandText = "SELECT firstname from people order by userid";
                using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tuples.Add(reader.GetString(0));
                    }
                }
                return tuples;
            });
            AssertFullNames(names);
        }
    }
}
