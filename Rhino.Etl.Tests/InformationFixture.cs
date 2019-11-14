using System.Threading.Tasks;

namespace Rhino.Etl.Tests
{
    using Fibonacci;
    using Xunit;

    
    public class InformationFixture 
    {
        [Fact]
        public async Task WillReportRowProcessedUsage()
        {
            InMemoryFibonacci fibonacci = new InMemoryFibonacci();
            await fibonacci.Execute();
            Assert.Equal(25, fibonacci.FibonacciOperation.Statistics.OutputtedRows);
        }

        [Fact]
        public async Task WillReportWhenOpeartionEnded()
        {
            bool finished = false;
            InMemoryFibonacci fibonacci = new InMemoryFibonacci();
            fibonacci.FibonacciOperation.OnFinishedProcessing += delegate
            {
                finished = true;
            };
            await fibonacci.Execute();
            Assert.True(finished);
        }
    }
}