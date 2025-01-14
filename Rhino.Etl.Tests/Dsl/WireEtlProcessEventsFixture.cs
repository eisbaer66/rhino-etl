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

    
    public class WireEtlProcessEventsFixture : BaseAggregationFixture
    {
        [Fact]
        public void CanCompileWithRowProcessedEvent()
        {
            using (EtlProcess process = CreateDslInstance("Dsl/WireRowProcessedEvent.boo"))
                Assert.NotNull(process);    
        }

        [Fact]
        public async Task CheckIfOnRowProcessedEventWasWired()
        {
            using (var process = CreateDslInstance("Dsl/WireRowProcessedEvent.boo"))
            {
                process.Register(new GenericEnumerableOperation(rows));
                ResultsToList operation = new ResultsToList();
                process.RegisterLast(operation);
                await process.Execute();
                Assert.Single(operation.Results);
                Assert.Equal("chocolate, sugar, coffee", operation.Results[0]["result"]);
            }
        }

        [Fact]
        public void CanCompileWithFinishedProcessingEvent()
        {
            using (var process = CreateDslInstance("Dsl/WireOnFinishedProcessingEvent.boo"))
                Assert.NotNull(process);
        }

        [Fact]
        public async Task CheckIfOnFinishedProcessingEventWasWired()
        {
            using (var process = CreateDslInstance("Dsl/WireOnFinishedProcessingEvent.boo"))
            {
                process.Register(new GenericEnumerableOperation(rows));
                ResultsToList operation = new ResultsToList();
                process.RegisterLast(operation);
                await process.Execute();
                Assert.Single(operation.Results);
                Assert.True(File.Exists(@"OnFinishedProcessing.wired"));

                File.Delete(@"OnFinishedProcessing.wired");
            }

        }
    }
}
