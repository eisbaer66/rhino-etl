using System.Data.Common;
using System.Threading.Tasks;

namespace Rhino.Etl.Tests.Infrastructure
{
    using System.Configuration;
    using System.Data;
    using System.Data.SqlClient;
    using Rhino.Etl.Core.Infrastructure;
    using Xunit;

    public class UseFixture
    {
        [Fact]
        public async Task SupportsAssemblyQualifiedConnectionTypeNameAsProviderNameInConnectionStringSettings()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;
            ConnectionStringSettings connectionStringSettings = new ConnectionStringSettings("test2", connectionString, typeof(SqlConnection).AssemblyQualifiedName);

            using (DbConnection connection = await Database.Connection(connectionStringSettings))
            {
                Assert.NotNull(connection);
            }
        }
 
        [Fact]
        public async Task SupportsProviderNameInConnectionStringSettings()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;
            ConnectionStringSettings connectionStringSettings = new ConnectionStringSettings("test2", connectionString, "System.Data.SqlClient");

            using (DbConnection connection = await Database.Connection(connectionStringSettings))
            {
                Assert.NotNull(connection);
            }
        }
    }
}
