using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Branches
{
    internal class BranchingOperationProcess<T> : EtlProcess where T : AbstractBranchingOperation, new()
    {
        protected override void Initialize()
        {
            Register(new GenerateTuples());
            Register(new T()
              .Add(Partial
                 .Register(new Add(false)))
              .Add(Partial
                 .Register(new Subtract(true))));
        }
    }

    internal class GenerateTuples : AbstractOperation
    {
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows, CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerable<Row>(async yield => {
                for (int i = 0; i < 2; i++)
                {
                    for (int j = 0; j < 2; j++)
                    {
                        var row = new Row();
                        row["a"] = i;
                        row["b"] = j;
                        await yield.ReturnAsync(row);
                    }
                }
            });
        }
    }

    internal class Subtract : AbstractOperationWithErrors
    {
        public Subtract(bool withErrors)
            : base(withErrors)
        {
        }

        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield yield)
        {
            var a = row["a"] as int?;
            var b = row["b"] as int?;
            row["operation"] = "subtract";
            row["result"] = a - b;
            if (withErrors)
            {
                Error(new ApplicationException(), "Error in Subtract");
            }

            await yield.ReturnAsync(row);
        }
    }

    internal abstract class AbstractOperationWithErrors : AbstractProcessingOperation
    {
        protected readonly bool withErrors;

        public AbstractOperationWithErrors(bool withErrors)
        {
            this.withErrors = withErrors;
        }
    }

    internal class Add : AbstractOperationWithErrors
    {
        public Add(bool withErrors)
            : base(withErrors)
        {
        }

        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield yield)
        {
            var a = row["a"] as int?;
            var b = row["b"] as int?;
            row["operation"] = "add";
            row["result"] = a + b;

            if (withErrors) { Error(new ApplicationException(), "Error in Add"); }

            await yield.ReturnAsync(row);
        }
    }
}