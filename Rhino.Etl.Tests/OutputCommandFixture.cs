using System.Threading.Tasks;

namespace Rhino.Etl.Tests
{
    using System;
    using System.Collections.Generic;
    using Xunit;
    using Fibonacci.Output;

    
    public class OutputCommandFixture : BaseFibonacciTest
    {
        [Fact]
        public async Task CanInsertToDatabaseFromInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(25, Should.WorkFine);
            await fibonaci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            await EnsureFibonacciTableExists();

            OutputFibonacciToDatabaseFromConnectionStringSettings fibonaci = new OutputFibonacciToDatabaseFromConnectionStringSettings(25, Should.WorkFine);
            await fibonaci.Execute();

            await Assert25ThFibonacci();
        }

        [Fact]
        public async Task WillRaiseRowProcessedEvent()
        {
            await EnsureFibonacciTableExists();

            int rowsProcessed = 0;

            using (OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(1, Should.WorkFine))
            {
                fibonaci.OutputOperation.OnRowProcessed += delegate { rowsProcessed++; };
                await fibonaci.Execute();
            }

            Assert.Equal(1, rowsProcessed);
        }

        [Fact]
        public async Task WillRaiseRowProcessedEventUntilItThrows()
        {
            await EnsureFibonacciTableExists();

            int rowsProcessed = 0;

            using (OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(25, Should.Throw))
            {
                fibonaci.OutputOperation.OnRowProcessed += delegate { rowsProcessed++; };
                await fibonaci.Execute();

                Assert.Equal(fibonaci.ThrowingOperation.RowsAfterWhichToThrow, rowsProcessed);
            }
        }

        [Fact]
        public async Task WillRaiseFinishedProcessingEventOnce()
        {
            await EnsureFibonacciTableExists();

            int finished = 0;

            using (OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(1, Should.WorkFine))
            {
                fibonaci.OutputOperation.OnFinishedProcessing += delegate { finished++; };
                await fibonaci.Execute();
            }

            Assert.Equal(1, finished);
        }

        [Fact]
        public async Task WhenErrorIsThrownWillRollbackTransaction()
        {
            await EnsureFibonacciTableExists();

            OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(25, Should.Throw);
            await fibonaci.Execute();
            Assert.Equal(1, new List<Exception>(fibonaci.GetAllErrors()).Count);
            await AssertFibonacciTableEmpty();
        }
    }
}
