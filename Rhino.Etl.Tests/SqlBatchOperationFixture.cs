using System.Threading.Tasks;

namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Xunit;
    using Rhino.Etl.Tests.Fibonacci.Batch;
    using Rhino.Etl.Tests.Fibonacci.Output;

    
    public class SqlBatchOperationFixture : BaseFibonacciTest
    {
        [Fact]
        public async Task CanInsertToDatabaseFromInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            BatchFibonacci fibonaci = new BatchFibonacci(25,Should.WorkFine);
            await fibonaci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromInMemoryCollectionWithSlowOperation()
        {
            await EnsureFibonacciTableExists();

            var fibonaci = new SlowBatchFibonacci(25, Should.WorkFine);
            await fibonaci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            BatchFibonacciFromConnectionStringSettings fibonaci = new BatchFibonacciFromConnectionStringSettings(25, Should.WorkFine);
            await fibonaci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task WhenErrorIsThrownWillRollbackTransaction()
        {
            await EnsureFibonacciTableExists();

            BatchFibonacci fibonaci = new BatchFibonacci(25, Should.Throw);
            await fibonaci.Execute();
            Assert.Single(new List<Exception>(fibonaci.GetAllErrors()));
            await AssertFibonacciTableEmpty();
        }
    }
}