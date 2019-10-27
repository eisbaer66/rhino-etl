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
            OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(25, Should.WorkFine);
            await fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public async Task CanInsertToDatabaseFromConnectionStringSettingsAndInMemoryCollection()
        {
            OutputFibonacciToDatabaseFromConnectionStringSettings fibonaci = new OutputFibonacciToDatabaseFromConnectionStringSettings(25, Should.WorkFine);
            await fibonaci.Execute();

            Assert25ThFibonacci();
        }

        [Fact]
        public async Task WillRaiseRowProcessedEvent()
        {
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
            OutputFibonacciToDatabase fibonaci = new OutputFibonacciToDatabase(25, Should.Throw);
            await fibonaci.Execute();
            Assert.Equal(1, new List<Exception>(fibonaci.GetAllErrors()).Count);
            AssertFibonacciTableEmpty();
        }
    }
}
