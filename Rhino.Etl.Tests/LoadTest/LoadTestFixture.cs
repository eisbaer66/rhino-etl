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

        public LoadTestFixture()
        {
            currentUserCount = GetUserCount("1 = 1");
        }

        public void AssertUpdatedAllRows()
        {
            Assert.Equal(expectedCount + currentUserCount, GetUserCount("testMsg is not null"));

        }

        private static int GetUserCount(string where)
        {
            return Use.Transaction<int>("test", delegate(IDbCommand command)
            {
                command.CommandText = "select count(*) from users where " + where;
                return (int)command.ExecuteScalar();
            });
        }

        [Fact]
        public async Task CanUpdateAllUsersToUpperCase()
        {
            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new UpdateUserNames());
                await update.Execute();
            }
            AssertUpdatedAllRows();
        }

        [Fact]
        public async Task CanBatchUpdateAllUsersToUpperCase()
        {
            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BatchUpdateUserNames());
                await update.Execute();
            }

            AssertUpdatedAllRows();
        }

        [Fact]
        public async Task BulkInsertUpdatedRows()
        {
            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                await push.Execute();
            if (expectedCount != GetUserCount("1 = 1"))
                return;//ignoring test

            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BulkInsertUsers());
                await update.Execute();
            }

            AssertUpdatedAllRows();
        }
    }
}