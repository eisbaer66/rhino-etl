using System.Configuration;
using System.Data.Common;
using System.Threading;
using Dasync.Collections;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core.Operations
{
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Generic input command operation
    /// </summary>
    public abstract class InputCommandOperation : AbstractCommandOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        public InputCommandOperation(string connectionStringName)
            : this(ConfigurationManager.ConnectionStrings[connectionStringName])
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputCommandOperation"/> class.
        /// </summary>
        /// <param name="connectionStringSettings">Connection string settings to use.</param>
        public InputCommandOperation(ConnectionStringSettings connectionStringSettings)
            : base(connectionStringSettings)
        {
            UseTransaction = true;
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
                using (DbConnection connection = await Database.Connection(ConnectionStringSettings, cancellationToken))
                using (DbTransaction transaction = BeginTransaction(connection))
                {
                    using (currentCommand = connection.CreateCommand())
                    {
                        currentCommand.Transaction = transaction;
                        PrepareCommand(currentCommand);
                        using (DbDataReader reader = await currentCommand.ExecuteReaderAsync(cancellationToken))
                        {
                            while (await reader.ReadAsync(cancellationToken))
                            {
                                await yield.ReturnAsync(CreateRowFromReader(reader));
                            }
                        }
                    }

                    if (transaction != null) transaction.Commit();
                }
            });
        }

        /// <summary>
        /// Creates a row from the reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        protected abstract Row CreateRowFromReader(IDataReader reader);

        /// <summary>
        /// Prepares the command for execution, set command text, parameters, etc
        /// </summary>
        /// <param name="cmd">The command.</param>
        protected abstract void PrepareCommand(IDbCommand cmd);
    }
}
