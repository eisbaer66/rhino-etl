using System.Configuration;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using Operations;
    using Pipelines;

    /// <summary>
    /// A single etl process
    /// </summary>
    public abstract class EtlProcess : EtlProcessBase<EtlProcess>, IDisposable
    {
        private IPipelineExecuter pipelineExecuter = new ThreadPoolPipelineExecuter();

        /// <summary>
        /// Gets the pipeline executer.
        /// </summary>
        /// <value>The pipeline executer.</value>
        public IPipelineExecuter PipelineExecuter
        {
            get { return pipelineExecuter; }
            set
            {
                Info("Setting PipelineExecutor to {PipelineExecutor}", value.GetType().ToString());
                pipelineExecuter = value;
            }
        }


        /// <summary>
        /// Gets a new partial process that we can work with
        /// </summary>
        protected static PartialProcessOperation Partial
        {
            get
            {
                PartialProcessOperation operation = new PartialProcessOperation();
                return operation;
            }
        }

        #region IDisposable Members

        ///<summary>
        ///Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        ///</summary>
        ///<filterpriority>2</filterpriority>
        public void Dispose()
        {
            foreach (IOperation operation in operations)
            {
                operation.Dispose();
            }
        }

        #endregion

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        protected abstract void Initialize();

        /// <summary>
        /// Executes this process
        /// </summary>
        public async Task Execute(CancellationToken cancellationToken = default)
        {
            Initialize();
            MergeLastOperationsToOperations();
            RegisterToOperationsEvents();
            Trace("Starting to execute {ProcessName}", Name);
            await PipelineExecuter.Execute(Name, operations, TranslateRows, cancellationToken);

            PostProcessing();
        }

        /// <summary>
        /// Translate the rows from one representation to another
        /// </summary>
        public virtual IAsyncEnumerable<Row> TranslateRows(IAsyncEnumerable<Row> rows)
        {
            return rows;
        }

        private void RegisterToOperationsEvents()
        {
            foreach (IOperation operation in operations)
            {
                operation.OnRowProcessed += OnRowProcessed;
                operation.OnFinishedProcessing += OnFinishedProcessing;
            }
        }


        /// <summary>
        /// Called when this process has finished processing.
        /// </summary>
        /// <param name="op">The op.</param>
        protected virtual void OnFinishedProcessing(IOperation op)
        {
            Trace("Finished {OperationName}: {@Statistics}", op.Name, op.Statistics);
        }

        /// <summary>
        /// Allow derived class to deal with custom logic after all the internal steps have been executed
        /// </summary>
        protected virtual void PostProcessing()
        {
        }

        /// <summary>
        /// Called when a row is processed.
        /// </summary>
        /// <param name="op">The operation.</param>
        /// <param name="dictionary">The dictionary.</param>
        protected virtual void OnRowProcessed(IOperation op, Row dictionary)
        {
            if (op.Statistics.OutputtedRows % 1000 == 0)
                Info("Processed {CountOfOutputtedRows} rows in {OperationName}", op.Statistics.OutputtedRows, op.Name);
            else
                Debug("Processed {CountOfOutputtedRows} rows in {OperationName}", op.Statistics.OutputtedRows, op.Name);
        }

        /// <summary>
        /// Executes the command and return a scalar
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionName">Name of the connection.</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns></returns>
        protected static async Task<T> ExecuteScalar<T>(string            connectionName,
                                            string            commandText,
                                            CancellationToken cancellationToken = default)
        {
            return await ExecuteScalar<T>(ConfigurationManager.ConnectionStrings[connectionName], commandText, cancellationToken);
        }

        /// <summary>
        /// Executes the command and return a scalar
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionStringSettings">The connection string settings node to use</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns></returns>
        protected static async Task<T> ExecuteScalar<T>(ConnectionStringSettings connectionStringSettings,
                                                        string                   commandText,
                                                        CancellationToken        cancellationToken = default)
        {
            return await Database.Transaction<T>(connectionStringSettings,
                                            async delegate(DbCommand cmd)
                                            {
                                                cmd.CommandText = commandText;
                                                object scalar = await cmd.ExecuteScalarAsync(cancellationToken);
                                                return (T) (scalar ?? default(T));
                                            },
                                            cancellationToken);
        }

        /// <summary>
        /// Gets all errors that occured during the execution of this process
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Exception> GetAllErrors()
        {
            foreach (Exception error in Errors)
            {
                yield return error;
            }
            foreach (Exception error in pipelineExecuter.GetAllErrors())
            {
                yield return error;
            }
            foreach (IOperation operation in operations)
            {
                foreach (Exception exception in operation.GetAllErrors())
                {
                    yield return exception;
                }
            }
        }
    }
}
