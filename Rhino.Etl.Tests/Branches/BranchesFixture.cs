using System;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Etl.Core;
using Xunit;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.Branches
{
    public abstract class BranchesFixture : BaseFibonacciTest
    {
        [Fact]
        public async Task CanBranchThePipeline()
        {
            await EnsureFibonacciTableExists();

            using (var process = CreateBranchingProcess(30, 2))
                await process.Execute();

            await AssertFibonacci(30, 2);
        }

        [Fact] 
        public async Task CanBranchThePipelineEfficiently()
        {
            await EnsureFibonacciTableExists();

            const int iterations = 30000;
            const int childOperations = 10;

            var initialMemory = GC.GetTotalMemory(true);

            using (var process = CreateBranchingProcess(iterations, childOperations))
                await process.Execute();

            var finalMemory = GC.GetTotalMemory(true);
            var consumedMemory = finalMemory - initialMemory;
            var tooMuchMemory = Math.Pow(2, 20);
            
            Assert.True(consumedMemory < tooMuchMemory, "Consuming too much memory - (" + consumedMemory.ToString() + " >= " + tooMuchMemory + ")");
            await AssertFibonacci(iterations, childOperations);
        }

        protected abstract EtlProcess CreateBranchingProcess(int iterations, int childOperations);

        protected static async Task AssertFibonacci(int iterations, int repetitionsPerIteration)
        {
            await AssertTotalItems(iterations * repetitionsPerIteration);

            await AssertRepetitions(repetitionsPerIteration);
        }

        private static async Task AssertRepetitions(int repetitionsPerIteration)
        {
            int wrongRepetitions = await Database.Transaction("test", async cmd =>
                                                           {
                                                               cmd.CommandText =
                                                                   string.Format(@"    SELECT count(*) 
    FROM (
        SELECT id, count(*) as count
        FROM Fibonacci
        GROUP BY id
        HAVING count(*) <> {0}
    ) as ignored", repetitionsPerIteration);
                                                               return (int)await cmd.ExecuteScalarAsync();
                                                           });

            Assert.Equal(1 /* 1 is repetated twice the others */, wrongRepetitions);
        }

        private static async Task AssertTotalItems(int expectedCount)
        {
            int totalCount = await Database.Transaction("test", async cmd =>
                                                     {
                                                         cmd.CommandText = "SELECT count(*) FROM Fibonacci";
                                                         return (int) await cmd.ExecuteScalarAsync();
                                                     });
            
            Assert.Equal(expectedCount, totalCount);
        }
    }
}