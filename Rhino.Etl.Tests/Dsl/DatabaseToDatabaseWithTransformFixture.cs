using System.Data.Common;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Core;
    using Xunit;

    
    public class DatabaseToDatabaseWithTransformFixture : BaseUserToPeopleTest
    {
        [Fact]
        public async Task CanCompile()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/UsersToPeople.boo"))
                Assert.NotNull(process);
        }

        [Fact]
        public async Task CanCopyTableWithTransform()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/UsersToPeople.boo"))
                await process.Execute();

            List<string[]> names = await Use.Transaction<List<string[]>>("test", async delegate(DbCommand cmd)
            {
                List<string[]> tuples = new List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }
    }
}