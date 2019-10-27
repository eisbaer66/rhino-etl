using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Rhino.Mocks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Rhino.Etl.Tests.Branches
{
    public class BranchingOperationFixture
    {
        [Fact]
        public async Task TheOldBranchingOperationDoesNotReportErrors()
        {
            using (var process = new BranchingOperationProcess<BranchingOperationWithBug>())
            {
                await process.Execute();
                var errors = process.GetAllErrors().Count();
                Assert.Equal(0, errors);
            }
        }

        [Fact]
        public async Task TheNewBranchingOperationReportsErrors()
        {
            using (var process = new BranchingOperationProcess<BranchingOperation>())
            {
                await process.Execute();
                var errors = process.GetAllErrors().Count();
                Assert.NotEqual(0, errors);
            }
        }
    }
}