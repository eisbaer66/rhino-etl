using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Tests.Joins
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Operations;

    public class AllStringsToUpperCase : AbstractProcessingOperation
    {
        protected override async Task ExecuteAsync(Row row, AsyncEnumerator<Row>.Yield @yield)
        {
            foreach (string column in row.Columns)
            {
                string item = row[column] as string;
                if(item!=null)
                    row[column] = item.ToUpper();
            }
            await yield.ReturnAsync(row);
        }
    }
}