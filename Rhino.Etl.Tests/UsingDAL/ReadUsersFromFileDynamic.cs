using System;
using System.Threading.Tasks;
using Dasync.Collections;
using FileHelpers;
using FileHelpers.RunTime;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using Core;
    using Rhino.Etl.Core.Files;
    using Rhino.Etl.Core.Operations;
    using System.Linq;

    public class ReadUsersFromFileDynamic : AbstractYieldOperation
    {
        private Type _tblClass;
        public ReadUsersFromFileDynamic()
        {
            var userRecordClassBuilder = new DelimitedClassBuilder("UserRecord","\t");
            userRecordClassBuilder.IgnoreFirstLines = 1;
            userRecordClassBuilder.AddField("Id", typeof(Int32));
            userRecordClassBuilder.AddField("Name", typeof(String));
            userRecordClassBuilder.AddField("Email", typeof(String));
            _tblClass = userRecordClassBuilder.CreateRecordClass();
        }

        protected override async Task ExecuteYield(IAsyncEnumerable<Row> rows, AsyncEnumerator<Row>.Yield @yield)
        {
            var file = new FileHelperEngine(_tblClass);
            //var ary = new[] {"one", "two", "three"};
            //var items = from a in ary select a;
            var items = file.ReadFile("users.txt");
            foreach (object obj in items)
            {
                await @yield.ReturnAsync(Row.FromObject(obj));
            }
        }
    }
}