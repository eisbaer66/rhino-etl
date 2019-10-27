using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Core
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Operations;

    /// <summary>
    /// Interface that abastract the actual execution of the pipeline
    /// </summary>
    public interface IPipelineExecuter
    {
        /// <summary>
        /// Executes the specified pipeline.
        /// </summary>
        /// <param name="pipelineName">The name.</param>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="translateRows">Translate the rows into another representation</param>
        /// <param name="cancellationToken"></param>
        Task Execute(string pipelineName,
            ICollection<IOperation> pipeline,
            Func<IAsyncEnumerable<Row>, IAsyncEnumerable<Row>> translateRows, CancellationToken cancellationToken);

        /// <summary>
        /// Transform the pipeline to an enumerable
        /// </summary>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="rows">The rows.</param>
        /// <param name="translateEnumerable">Translate the rows from one representation to another</param>
        /// <returns></returns>
        IAsyncEnumerable<Row> PipelineToEnumerable(
            ICollection<IOperation> pipeline,
            IAsyncEnumerable<Row> rows,
            Func<IAsyncEnumerable<Row>, IAsyncEnumerable<Row>> translateEnumerable);

        /// <summary>
        /// Gets all errors that occured under this executer
        /// </summary>
        /// <returns></returns>
        IEnumerable<Exception> GetAllErrors();

        /// <summary>
        /// Gets a value indicating whether this instance has errors.
        /// </summary>
        /// <value>
        ///     <c>true</c> if this instance has errors; otherwise, <c>false</c>.
        /// </value>
        bool HasErrors { get; }
    }
}