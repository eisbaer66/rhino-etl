using System.Data.Common;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.Dsl
{
    using System.Collections.Generic;
    using System.Data;
    using Core;
    using Xunit;
    using Rhino.Etl.Dsl;

    
    public class HashJoinFixture : BaseUserToPeopleTest
    {
        [Fact]
        public async Task CanCompile()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/InnerHashJoin.boo"))
                Assert.NotNull(process);
        }

        [Fact]
        public async Task CanWriteJoinsToDatabase()
        {
            await SetupTables();

            using (EtlProcess process = CreateDslInstance("Dsl/InnerHashJoin.boo"))
                await process.Execute();
            List<string> roles = new List<string>();
            await Database.Transaction("test", async delegate(DbCommand command)
            {
                command.CommandText = @"
                                SELECT Roles FROM Users
                                WHERE Roles IS NOT NULL
                                ORDER BY Id
                ";
                using(DbDataReader reader = await command.ExecuteReaderAsync())
                    while(await reader.ReadAsync())
                    {
                        roles.Add(reader.GetString(0));
                    }
            });
            Assert.Equal("ayende rahien is: admin, janitor, employee, customer", roles[0]);
            Assert.Equal("foo bar is: janitor", roles[1]);
            Assert.Equal("gold silver is: janitor, employee", roles[2]);
        }
    }
}