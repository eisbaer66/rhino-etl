using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Dsl
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Aggregation;
    using Core;
    using Joins;
    using Xunit;
    using Rhino.Etl.Core.Operations;
    using Rhino.Etl.Dsl;

    
    public class AggregationFixture : BaseAggregationFixture
    {
        [Fact]
        public void CanCompile()
        {
            EtlProcess process = CreateDslInstance("Dsl/Aggregate.boo");
            Assert.NotNull(process);
        }


        [Fact]
        public async Task CanPerformAggregationFromDsl()
        {
            EtlProcess process = CreateDslInstance("Dsl/Aggregate.boo");
            process.Register(new GenericEnumerableOperation(rows));
            ResultsToList operation = new ResultsToList();
            process.RegisterLast(operation);
            await process.Execute();
            Assert.Single(operation.Results);
            Assert.Equal("milk, sugar, coffee", operation.Results[0]["result"]);
        }
    }
}