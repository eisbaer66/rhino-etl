using Rhino.Etl.Core.Enumerables;
using System.Linq;
using Dasync.Collections;

namespace Rhino.Etl.Core.Operations
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Branch the current pipeline flow into all its inputs
    /// </summary>
    public class BranchingOperation : AbstractBranchingOperation
    {
        /// <summary>
        /// Executes this operation, sending the input of this operation
        /// to all its child operations
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows)
        {
            return new AsyncEnumerable<Row>(async yield => {
                var copiedRows = new CachingEnumerable<Row>(rows);

                foreach (IOperation operation in Operations)
                {
                    var cloned = copiedRows.Select(r => r.Clone());

                    IAsyncEnumerable<Row> enumerable = operation.Execute(cloned);

                    if (enumerable == null)
                        continue;

                    IAsyncEnumerator<Row> enumerator = enumerable.GetAsyncEnumerator();
#pragma warning disable 642
                    while (await enumerator.MoveNextAsync()) ;
#pragma warning restore 642
                }
                yield.Break();
            });
        }

        /// <summary>
        /// Get All Errors from child operations
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<Exception> GetAllErrors()
        {
            foreach (var operation in Operations)
            {
                foreach (var error in operation.GetAllErrors())
                {
                    yield return error;
                }
            }

            foreach (var error in base.GetAllErrors())
            {
                yield return error;
            }
        }
    }
}