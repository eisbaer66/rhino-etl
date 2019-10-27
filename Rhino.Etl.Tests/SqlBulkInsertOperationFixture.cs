using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Rhino.Etl.Core.Operations;
using Rhino.Mocks;

namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Xunit;
    using Rhino.Etl.Tests.Fibonacci.Bulk;
    using Rhino.Etl.Tests.Fibonacci.Output;

    
    public class SqlBulkInsertOperationFixture : BaseFibonacciTest
    {
        [Fact]
        public async Task CanInsertToDatabaseFromInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            BulkInsertFibonacciToDatabase fibonacci = new BulkInsertFibonacciToDatabase(25,Should.WorkFine);
            await fibonacci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            BulkInsertFibonacciToDatabaseFromConnectionStringSettings fibonacci = new BulkInsertFibonacciToDatabaseFromConnectionStringSettings(25, Should.WorkFine);
            await fibonacci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task WhenErrorIsThrownWillRollbackTransaction()
        {
            await EnsureFibonacciTableExists();

            BulkInsertFibonacciToDatabase fibonaci = new BulkInsertFibonacciToDatabase(25, Should.Throw);
            await fibonaci.Execute();
            Assert.Equal(1, new List<Exception>(fibonaci.GetAllErrors()).Count);
            await AssertFibonacciTableEmpty();
        }
    }

    public class BulkInsertNotificationTests
    {
        [Fact]
        public void    CheckNotifyBatchSizeTakenFromBatchSize()
        {
            FibonacciBulkInsert    fibonacci =    new    FibonacciBulkInsert();
            fibonacci.BatchSize    = 50;

            Assert.Equal(fibonacci.BatchSize, fibonacci.NotifyBatchSize);
        }

        [Fact]
        public void    CheckNotifyBatchSizeNotTakenFromBatchSize()
        {
            FibonacciBulkInsert    fibonacci =    new    FibonacciBulkInsert();
            fibonacci.BatchSize    = 50;
            fibonacci.NotifyBatchSize =    25;

            Assert.Equal(25, fibonacci.NotifyBatchSize);
        }
    }
}
