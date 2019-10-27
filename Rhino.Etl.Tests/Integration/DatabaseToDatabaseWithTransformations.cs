using System.Data.Common;
using System.Threading.Tasks;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.Integration
{
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Data;
    using Boo.Lang;
    using Core;
    using Xunit;
    using Rhino.Etl.Core.Operations;

    
    public class DatabaseToDatabaseWithTransformations : BaseUserToPeopleTest
    {
        [Fact]
        public async Task CanCopyTableWithTransform()
        {
            await SetupTables();

            using (UsersToPeople process = new UsersToPeople())
                await process.Execute();
            
            System.Collections.Generic.List<string[]> names = await Use.Transaction("test", async delegate(DbCommand cmd)
            {
                System.Collections.Generic.List<string[]> tuples = new System.Collections.Generic.List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    while(await reader.ReadAsync())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }

        [Fact]
        public async Task CanCopyTableWithTransformFromConnectionStringSettings()
        {
            await SetupTables();

            using (UsersToPeopleFromConnectionStringSettings process = new UsersToPeopleFromConnectionStringSettings())
                await process.Execute();

            System.Collections.Generic.List<string[]> names = await Use.Transaction("test", async delegate(DbCommand cmd)
            {
                System.Collections.Generic.List<string[]> tuples = new System.Collections.Generic.List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            AssertNames(names);
        }       
    }
}