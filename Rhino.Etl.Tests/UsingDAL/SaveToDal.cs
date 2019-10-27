using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class SaveToDal : AbstractProcessingOperation
    {
        protected override Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield)
        {
            MySimpleDal.Save(row.ToObject<User>());
            @yield.Break();

            return Task.CompletedTask;
        }
    }
}