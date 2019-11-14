using System.Data.Common;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.LoadTest
{
    using System.Data;
    using Core;
    using Xunit;
    using Rhino.Etl.Core.Operations;

    /// <summary>
    /// This fixture is here to verify that we can handle large amount of data
    /// without consuming too much memory or crashing
    /// </summary>
    public class LoadTestFixture : BaseUserToPeopleTest
    {
        private const int expectedCount = 5000;
        private int currentUserCount;

        private async Task AssertUpdatedAllRows()
        {
            Assert.Equal(expectedCount + currentUserCount, await GetUserCount("testMsg is not null"));

        }

        private static async Task<int> GetUserCount(string where)
        {
            return await Database.Transaction("test", async delegate(DbCommand command)
            {
                command.CommandText = "select count(*) from users where " + where;
                return (int)await command.ExecuteScalarAsync();
            });
        }

        [Fact]
        public async Task CanUpdateAllUsersToUpperCase()
        {
            await SetupTables();
            currentUserCount = await GetUserCount("1 = 1");

            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new UpdateUserNames());
                await update.Execute();
            }
            await AssertUpdatedAllRows();
        }

        [Fact]
        public async Task CanBatchUpdateAllUsersToUpperCase()
        {
            await SetupTables();
            currentUserCount = await GetUserCount("1 = 1");

            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BatchUpdateUserNames());
                await update.Execute();
            }

            await AssertUpdatedAllRows();
        }

        [Fact]
        public async Task BulkInsertUpdatedRows()
        {
            await SetupTables();
            currentUserCount = await GetUserCount("1 = 1");

            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            if (expectedCount != await GetUserCount("1 = 1"))
                return;//ignoring test

            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BulkInsertUsers());
                await update.Execute();
            }

            await AssertUpdatedAllRows();
        }
    }
}