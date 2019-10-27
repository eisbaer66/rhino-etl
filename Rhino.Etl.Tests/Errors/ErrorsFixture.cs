using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Errors
{
    using System;
    using System.Collections.Generic;
    using Core;
    using Joins;
    using Xunit;

    
    public class ErrorsFixture : BaseFibonacciTest
    {
        [Fact]
        public async Task WillReportErrorsWhenThrown()
        {
            await EnsureFibonacciTableExists();

            using (ErrorsProcess process = new ErrorsProcess())
            {
                ICollection<Row> results = new List<Row>();
                process.RegisterLast(new AddToResults(results));

                await process.Execute();
                Assert.Equal(process.ThrowOperation.RowsAfterWhichToThrow, results.Count);
                List<Exception> errors = new List<Exception>(process.GetAllErrors());
                Assert.Equal(1, errors.Count);
                Assert.Equal("Failed to execute operation Rhino.Etl.Tests.Errors.ThrowingOperation: problem",
                                errors[0].Message);
            }
        }

        [Fact]
        public async Task OutputCommandWillRollbackTransactionOnError()
        {
            await EnsureFibonacciTableExists();

            using (ErrorsProcess process = new ErrorsProcess())
            {
              
                
            }
        }
    }
}
