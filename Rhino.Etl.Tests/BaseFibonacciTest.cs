using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Xunit;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests
{
    public class BaseFibonacciTest
    {
        protected static async Task EnsureFibonacciTableExists()
        {
            await Use.Transaction("test", async delegate (DbCommand cmd)
                                    {
                                        cmd.CommandText =
                                            @"
if object_id('Fibonacci') is not null
    drop table Fibonacci
create table Fibonacci ( id int );
";
                                        await cmd.ExecuteNonQueryAsync();
                                    });
        }

        protected static async Task Assert25ThFibonacci()
        {
            int max = await Use.Transaction("test", async delegate(DbCommand cmd)
            {
                cmd.CommandText = "SELECT MAX(id) FROM Fibonacci";
                return (int) await cmd.ExecuteScalarAsync();
            });
            Assert.Equal(75025, max);
        }

        protected static async Task AssertFibonacciTableEmpty()
        {
            int count = await Use.Transaction("test", async delegate(DbCommand cmd)
            {
                cmd.CommandText = "SELECT count(id) FROM Fibonacci";
                return (int) await cmd.ExecuteScalarAsync();
            });
            Assert.Equal(0, count);
        }
    }
}