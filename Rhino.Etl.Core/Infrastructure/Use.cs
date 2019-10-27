using System;
using System.Configuration;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Etl.Core.Infrastructure
{
    using System.Data.Common;

    /// <summary>
    /// Helper class to provide simple data access, when we want to access the ADO.Net
    /// library directly. 
    /// </summary>
    public static class Use
    {
        #region Delegates

        /// <summary>
        /// Delegate to execute an action with a command
        /// and return a result: <typeparam name="T"/>
        /// </summary>
        public delegate Task<T> Func<T>(DbCommand command);

        /// <summary>
        /// Delegate to execute an action with a command
        /// </summary>
        public delegate Task Proc(DbCommand command);

        #endregion

        /// <summary>
        /// Gets or sets the active connection.
        /// </summary>
        /// <value>The active connection.</value>
        [ThreadStatic]
        private static DbConnection ActiveConnection;

        /// <summary>
        /// Gets or sets the active transaction.
        /// </summary>
        /// <value>The active transaction.</value>
        [ThreadStatic] 
        private static DbTransaction ActiveTransaction;

        /// <summary>
        /// Gets or sets the transaction counter.
        /// </summary>
        /// <value>The transaction counter.</value>
        [ThreadStatic]
        private static int TransactionCounter;

        /// <summary>
        /// Execute the specified delegate inside a transaction and return 
        /// the result of the delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionStringName">The name of the named connection string in the configuration file</param>
        /// <param name="actionToExecute">The action to execute</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns></returns>
        public static async Task<T> Transaction<T>(string            connectionStringName,
                                                   Func<T>           actionToExecute,
                                                   CancellationToken cancellationToken = default)
        {
            T result = default(T);

            ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connnection string: " + connectionStringName);

            await Transaction(connectionStringSettings, async delegate(DbCommand command) { result = await actionToExecute(command); }, cancellationToken);
            return result;
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction and return 
        /// the result of the delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionStringSettings">The connection string settings to use for the connection</param>
        /// <param name="actionToExecute">The action to execute</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns></returns>
        public static async Task<T> Transaction<T>(ConnectionStringSettings connectionStringSettings,
                                                   Func<T>                  actionToExecute,
                                                   CancellationToken        cancellationToken = default)
        {
            T result = default(T);
            await Transaction(connectionStringSettings, async delegate(DbCommand command) { result = await actionToExecute(command); }, cancellationToken);
            return result;
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        public static async Task Transaction(string            connectionStringName,
                                             Proc              actionToExecute,
                                             CancellationToken cancellationToken = default)
        {
            ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connection string: " + connectionStringName);

            await Transaction(connectionStringSettings, IsolationLevel.Unspecified, actionToExecute, cancellationToken);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction
        /// </summary>
        /// <param name="connectionStringSettings">The connection string settings to use for the connection</param>
        /// <param name="actionToExecute">The action to execute.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        public static async Task Transaction(ConnectionStringSettings connectionStringSettings,
                                             Proc                     actionToExecute,
                                             CancellationToken        cancellationToken = default)
        {
            await Transaction(connectionStringSettings, IsolationLevel.Unspecified, actionToExecute, cancellationToken);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction with the specific
        /// isolation level 
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="isolationLevel">The isolation level.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        public static async Task Transaction(string            connectionStringName,
                                             IsolationLevel    isolationLevel,
                                             Proc              actionToExecute,
                                             CancellationToken cancellationToken = default)
        {
            ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connection string: " + connectionStringName);

            await Transaction(connectionStringSettings, isolationLevel, actionToExecute, cancellationToken);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction with the specific
        /// isolation level 
        /// </summary>
        /// <param name="connectionStringSettings">Connection string settings node to use for the connection</param>
        /// <param name="isolationLevel">The isolation level.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        public static async Task Transaction(ConnectionStringSettings connectionStringSettings,
                                             IsolationLevel           isolationLevel,
                                             Proc                     actionToExecute,
                                             CancellationToken        cancellationToken = default)
        {
            await StartTransaction(connectionStringSettings, isolationLevel, cancellationToken);
            try
            {
                using (DbCommand command = ActiveConnection.CreateCommand())
                {
                    command.Transaction = ActiveTransaction;
                    await actionToExecute(command);
                }
                CommitTransaction();
            }
            catch
            {
                RollbackTransaction();
                throw;
            }
            finally
            {
                DisposeTransaction();
            }
        }

        /// <summary>
        /// Disposes the transaction.
        /// </summary>
        private static void DisposeTransaction()
        {
            if (TransactionCounter <= 0)
            {
                ActiveConnection.Dispose();
                ActiveConnection = null;
            }
        }

        /// <summary>
        /// Rollbacks the transaction.
        /// </summary>
        private static void RollbackTransaction()
        {
            ActiveTransaction.Rollback();
            ActiveTransaction.Dispose();
            ActiveTransaction = null;
            TransactionCounter = 0;
        }

        /// <summary>
        /// Commits the transaction.
        /// </summary>
        private static void CommitTransaction()
        {
            TransactionCounter--;
            if (TransactionCounter == 0 && ActiveTransaction != null)
            {
                ActiveTransaction.Commit();
                ActiveTransaction.Dispose();
                ActiveTransaction = null;
            }
        }

        /// <summary>
        /// Starts the transaction.
        /// </summary>
        /// <param name="connectionStringSettings">The connection string settings to use for the transaction</param>
        /// <param name="isolation">The isolation.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        private static async Task StartTransaction(ConnectionStringSettings connectionStringSettings,
                                                   IsolationLevel           isolation,
                                                   CancellationToken        cancellationToken = default)
        {
            if (TransactionCounter <= 0)
            {
                TransactionCounter = 0;
                ActiveConnection = await Connection(connectionStringSettings, cancellationToken);
                ActiveTransaction = ActiveConnection.BeginTransaction(isolation);
            }
            TransactionCounter++;
        }

        /// <summary>
        /// Creates an open connection for a given named connection string, using the provider name
        /// to select the proper implementation
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>The open connection</returns>
        public static async Task<DbConnection> Connection(string name, CancellationToken cancellationToken = default)
        {
            ConnectionStringSettings connectionString = ConfigurationManager.ConnectionStrings[name];
            if (connectionString == null)
                throw new InvalidOperationException("Could not find connnection string: " + name);

            return await Connection(connectionString, cancellationToken);
        }

        /// <summary>
        /// Creates an open connection for a given connection string setting, using the provider
        /// name of select the proper implementation
        /// </summary>
        /// <param name="connectionString">ConnectionStringSetting node</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>The open connection</returns>
        public static async Task<DbConnection> Connection(ConnectionStringSettings connectionString,
                                                          CancellationToken        cancellationToken = default)
        {
            if (connectionString == null)
                throw new InvalidOperationException("Null ConnectionStringSettings specified");
            if (connectionString.ProviderName == null)
                throw new InvalidOperationException("Null ProviderName specified");

            string providerName = connectionString.ProviderName;
            DbConnection connection = DbProviderFactories.GetFactory(providerName).CreateConnection();
            if (connection == null)
                throw new InvalidOperationException("DbProviderFactories could not create DbConnection from ProviderName " + providerName);

            connection.ConnectionString = connectionString.ConnectionString;
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
    }
}