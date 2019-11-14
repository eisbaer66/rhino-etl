using System.Linq;
using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Dsl
{
    using Aggregation;
    using Core;
    using Xunit;


    public class ImportFixture : BaseDslTest
    {
        [Fact]
        public void CanCompile()
        {
            EtlProcess process = CreateDslInstance("Dsl/Import.boo");
            Assert.NotNull(process);
        }


        [Fact]
        public async Task CanPerformExternalOperationFromDsl()
        {
            EtlProcess process = CreateDslInstance("Dsl/Import.boo");
            ResultsToList operation = new ResultsToList();
            process.RegisterLast(operation);
            await process.Execute();
            Assert.Equal(10, operation.Results.Count);
            Assert.Equal("1, 1, 2, 3, 5, 8, 13, 21, 34, 55", string.Join(", ", operation.Results.Select(r => r["id"]).ToArray()));
        }
    }
}