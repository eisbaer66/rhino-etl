using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Xunit;

namespace Rhino.Etl.Tests.Joins
{
    public class JoinWithPipelineFixture : BaseJoinFixture
    {
        [Fact]
        public async Task InnerJoinWithDefaultLeftJoin()
        {
            var items = new List<Row>();

            var result = new TestProcess(
                new GenericEnumerableOperation(left),
                new InnerJoinUsersToPeopleByEmail().Right(new GenericEnumerableOperation(right)),
                new ResultsOperation(items)
                );

            await result.Execute();

            Assert.Single(items);
            Assert.Equal(3, items[0]["person_id"]);
        }

        [Fact]
        public async Task RightJoinWithDefaultLeftJoin()
        {
            var items = new List<Row>();

            var result = new TestProcess(
                new GenericEnumerableOperation(left),
                new RightJoinUsersToPeopleByEmail().Right(new GenericEnumerableOperation(right)),
                new ResultsOperation(items)
                );

            await result.Execute();

            Assert.Equal(2, items.Count);
            Assert.Equal(3, items[0]["person_id"]);
            Assert.Null(items[1]["name"]);
            Assert.Equal(5, items[1]["person_id"]);
        }

        [Fact]
        public async Task LeftJoinWithDefaultLeftJoin()
        {
            var items = new List<Row>();

            var result = new TestProcess(
                new GenericEnumerableOperation(left),
                new LeftJoinUsersToPeopleByEmail().Right(new GenericEnumerableOperation(right)),
                new ResultsOperation(items)
                );

            await result.Execute();

            Assert.Equal(2, items.Count);
            Assert.Equal(3, items[0]["person_id"]);
            Assert.Null(items[1]["person_id"]);
            Assert.Equal("bar", items[1]["name"]);
        }

        [Fact]
        public async Task FullJoinWithDefaultLeftJoin()
        {
            var items = new List<Row>();

            var result = new TestProcess(
                new GenericEnumerableOperation(left),
                new FullJoinUsersToPeopleByEmail().Right(new GenericEnumerableOperation(right)),
                new ResultsOperation(items)
                );

            await result.Execute();

            Assert.Equal(3, items.Count);

            Assert.Equal(3, items[0]["person_id"]);

            Assert.Null(items[1]["person_id"]);
            Assert.Equal("bar", items[1]["name"]);

            Assert.Null(items[2]["name"]);
            Assert.Equal(5, items[2]["person_id"]);
        }

        protected class TestProcess : EtlProcess
        {
            private readonly List<IOperation> _operations;

            public TestProcess(params IOperation[] operations)
            {
                _operations = new List<IOperation>(operations);
            }

            protected override void Initialize()
            {
                _operations.ForEach(o => Register(o));
            }
        }

        protected class ResultsOperation : AbstractOperation
        {
            public ResultsOperation(List<Row> returnRows)
            {
                this.returnRows = returnRows;
            }

            List<Row> returnRows = null;

            public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows,
                CancellationToken cancellationToken = default)
            {
                return new AsyncEnumerable<Row>(async yield =>
                {
                    await rows.ForEachAsync(async r =>
                    {
                        returnRows.Add(r);
                        await yield.ReturnAsync(r);
                    }, cancellationToken);

                });
            }
        }
    }
}