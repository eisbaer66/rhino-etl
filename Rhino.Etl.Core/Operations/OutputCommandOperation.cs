using System.Configuration;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Dasync.Collections;
using Rhino.Etl.Core.Enumerables;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core.Operations
{
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Generic output command operation
    /// </summary>
    public abstract class OutputCommandOperation : AbstractCommandOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        public OutputCommandOperation(string connectionStringName) : this(ConfigurationManager.ConnectionStrings[connectionStringName])
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringSettings">Connection string settings to use.</param>
        public OutputCommandOperation(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="cancellationToken">A CancellationToken to stop execution</param>
        /// <returns></returns>
        public override IAsyncEnumerable<Row> Execute(IAsyncEnumerable<Row> rows, CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerable<Row>(async yield => {
                using (DbConnection connection = await Use.Connection(ConnectionStringSettings, cancellationToken))
                using (DbTransaction transaction = BeginTransaction(connection))
                {
                    foreach (Row row in new SingleRowEventRaisingEnumerator(this, rows))
                    {
                        using (DbCommand cmd = connection.CreateCommand())
                        {
                            currentCommand = cmd;
                            currentCommand.Transaction = transaction;
                            PrepareCommand(currentCommand, row);
                            await currentCommand.ExecuteNonQueryAsync(cancellationToken);
                        }
                    }
                    if (PipelineExecuter.HasErrors)
                    {
                        Warn("Rolling back transaction in {OperationName}", Name);
                        if (transaction != null) transaction.Rollback();
                        Warn("Rolled back transaction in {OperationName}", Name);
                    }
                    else
                    {
                        Debug("Committing {OperationName}", Name);
                        if (transaction != null) transaction.Commit();
                        Debug("Committed {OperationName}", Name);
                    }
                }
                yield.Break();
            });
        }

        /// <summary>
        /// Prepares the command for execution, set command text, parameters, etc
        /// </summary>
        /// <param name="cmd">The command.</param>
        /// <param name="row">The row.</param>
        protected abstract void PrepareCommand(IDbCommand cmd, Row row);
    }
}
