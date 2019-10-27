using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.LoadTest
{
    using System.Collections.Generic;
    using Core;
    using Core.Operations;

    public class AccumulateResults : AbstractProcessingOperation
    {
        public int count = 0;

        protected override Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield)
        {
            count += 1;

            return Task.CompletedTask;
        }
    }
}