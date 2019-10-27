using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Joins
{
    using Xunit;

    
    public class JoinInProcessFixture : BaseJoinFixture
    {
        [Fact]
        public async Task CanUseJoinInProcess()
        {
            using (TrivialUsersToPeopleJoinProcess process = new TrivialUsersToPeopleJoinProcess(left, right))
            {
                await process.Execute();
                Assert.Equal(1, process.Results.Count);
                Assert.Equal(3, process.Results[0]["person_id"]);
            }
        }

        [Fact]
        public async Task CanUseComplexJoinInProcesses()
        {
            using (ComplexUsersToPeopleJoinProcess process = new ComplexUsersToPeopleJoinProcess(left, right))
            {
                await process.Execute();
                Assert.Equal(2, process.Results.Count);
                Assert.Equal("FOO", process.Results[0]["name"]);
                Assert.Equal("FOO@EXAMPLE.ORG", process.Results[0]["email"]);
            }
        }
    }
}