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
            BatchFibonacci fibonaci = new BatchFibonacci(25,Should.WorkFine);
            await fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromInMemoryCollectionWithSlowOperation()
        {
            var fibonaci = new SlowBatchFibonacci(25, Should.WorkFine);
            await fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            BatchFibonacciFromConnectionStringSettings fibonaci = new BatchFibonacciFromConnectionStringSettings(25, Should.WorkFine);
            await fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public async Task WhenErrorIsThrownWillRollbackTransaction()
        {
            BatchFibonacci fibonaci = new BatchFibonacci(25, Should.Throw);
            await fibonaci.Execute();
            Assert.Equal(1, new List<Exception>(fibonaci.GetAllErrors()).Count);
            AssertFibonacciTableEmpty();
        }
    }
}