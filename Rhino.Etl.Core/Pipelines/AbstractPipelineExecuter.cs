using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;

namespace Rhino.Etl.Core.Pipelines
{
    using System;
    using System.Collections.Generic;
    using Operations;

    /// <summary>
    /// Base class for pipeline executers, handles all the details and leave the actual
    /// pipeline execution to the 
    /// </summary>
    public abstract class AbstractPipelineExecuter : WithLoggingMixin, IPipelineExecuter
    {
        #region IPipelineExecuter Members

        /// <summary>
        /// Executes the specified pipeline.
        /// </summary>
        /// <param name="pipelineName">The name.</param>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="translateRows">Translate the rows into another representation</param>
        /// <param name="cancellationToken"></param>
        public async Task Execute(string pipelineName,
            ICollection<IOperation> pipeline,
            Func<IAsyncEnumerable<Row>, IAsyncEnumerable<Row>> translateRows,
            CancellationToken cancellationToken = default)
        {
            try
            {
                IAsyncEnumerable<Row> enumerablePipeline = PipelineToEnumerable(pipeline, new List<Row>().ToAsyncEnumerable(), translateRows, cancellationToken);
                try
                {
                    raiseNotifyExecutionStarting();
                    DateTime start = DateTime.Now;
                    await ExecutePipeline(enumerablePipeline, cancellationToken);
                    raiseNotifyExecutionCompleting();
                    Trace("Completed process {PipelineName} in {ExecutionTime}", pipelineName, DateTime.Now - start);
                }
                catch (Exception e)
                {
                    Error(e, "Failed to execute pipeline {0}", new Tuple<string, object>("PipelineName", pipelineName));
                }
            }
            catch (Exception e)
            {
                Error(e, "Failed to create pipeline {0}", new Tuple<string, object>("PipelineName", pipelineName));                
            }

            DisposeAllOperations(pipeline);
        }

        /// <summary>
        /// Transform the pipeline to an enumerable
        /// </summary>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="rows">The rows</param>
        /// <param name="translateEnumerable">Translate the rows from one representation to another</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        public virtual IAsyncEnumerable<Row> PipelineToEnumerable(
            ICollection<IOperation> pipeline,
            IAsyncEnumerable<Row> rows,
            Func<IAsyncEnumerable<Row>, IAsyncEnumerable<Row>> translateEnumerable, 
            CancellationToken cancellationToken = default)
        {
            IList<Task> tasks = new List<Task>();
            foreach (var operation in pipeline)
            {
                operation.PrepareForExecution(this);
                var enumerator = operation.Execute(rows, cancellationToken);
                enumerator = translateEnumerable(enumerator);
                AsyncEnumerableTask<Row> task = DecorateEnumerableForExecution(operation, enumerator, cancellationToken);
                rows = task.Enumerable;
                tasks.Add(task.Task);
            }

            Task.WaitAll(tasks.ToArray());

            return rows;
        }

        /// <summary>
        /// Gets all errors that occured under this executer
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Exception> GetAllErrors()
        {
            return Errors;
        }

        /// <summary>
        /// Gets a value indicating whether this instance has errors.
        /// </summary>
        /// <value>
        ///     <c>true</c> if this instance has errors; otherwise, <c>false</c>.
        /// </value>
        public bool HasErrors
        {
            get { return Errors.Length != 0; }
        }

        #endregion

        /// <summary>
        /// Iterates the specified enumerable.
        /// Since we use a pipeline, we need to force it to execute at some point. 
        /// We aren't really interested in the result, just in that the pipeline would execute.
        /// </summary>
        protected virtual async Task ExecutePipeline(IAsyncEnumerable<Row> pipeline,
            CancellationToken cancellationToken = default)
        {
            var enumerator = pipeline.GetAsyncEnumerator(cancellationToken);
            try
            {
#pragma warning disable 642
                while (await enumerator.MoveNextAsync()) ;
#pragma warning restore 642
            }
            catch (Exception e)
            {
                Error(e, "Failed to execute operation {0}", new Tuple<string, object>("@Row", enumerator.Current));
            }
        }


        /// <summary>
        /// Destroys the pipeline.
        /// </summary>
        protected void DisposeAllOperations(ICollection<IOperation> operations)
        {
            foreach (IOperation operation in operations)
            {
                try
                {
                    operation.Dispose();
                }
                catch (Exception e)
                {
                    Error(e, "Failed to disposed {0}", new Tuple<string, object>("OperationName", operation.Name));
                }
            }
        }

        ///    <summary>
        ///    Occurs when    the    pipeline has been successfully created,    but    before it is executed
        ///    </summary>
        public event Action<IPipelineExecuter> NotifyExecutionStarting = delegate {    };

        ///    <summary>
        ///    Raises the ExecutionStarting event
        ///    </summary>
        private    void raiseNotifyExecutionStarting()
        {
            NotifyExecutionStarting(this);
        }

        ///    <summary>
        ///    Occurs when    the    pipeline has been successfully created,    but    before it is disposed
        ///    </summary>
        public event Action<IPipelineExecuter> NotifyExecutionCompleting = delegate    { };

        ///    <summary>
        ///    Raises the ExecutionCompleting event
        ///    </summary>
        private    void raiseNotifyExecutionCompleting()
        {
            NotifyExecutionCompleting(this);
        }

        ///    <summary>
        /// Add a decorator to the enumerable for additional processing
        /// </summary>
        ///    <param name="operation">The operation.</param>
        ///    <param name="enumerator">The enumerator.</param>
        ///    <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
        protected abstract AsyncEnumerableTask<Row> DecorateEnumerableForExecution(
            IOperation            operation,
            IAsyncEnumerable<Row> enumerator,
            CancellationToken     cancellationToken = default);
    }
}