using System;
using System.Threading.Tasks;

namespace Rhino.Etl.Tests.UsingDAL
{
    using System.Collections.Generic;
    using System.IO;
    using Core;
    using Xunit;

    
    public class UsingDALFixture
    {
        private const string expected =
            @"Id	Name	Email
1	ayende	ayende@example.org
2	foo	foo@example.org
3	bar	bar@example.org
4	brak	brak@example.org
5	snar	snar@example.org
";
        [Fact]
        public async Task CanWriteToFileFromDAL()
        {
            ExportUsersToFile export = new ExportUsersToFile();
            await export.Execute();
            string actual = File.ReadAllText("users.txt");
            Assert.Equal(expected.Replace("\r\n","\n").Replace("\n",Environment.NewLine), actual);
        }

        [Fact]
        public async Task CanReadFromFileToDAL()
        {
            MySimpleDal.Users = new List<User>();
            File.WriteAllText("users.txt", expected);

            ImportUsersFromFile import = new ImportUsersFromFile();
            await import.Execute();

            Assert.Equal(5, MySimpleDal.Users.Count);
        }

        [Fact]
        public async Task CanReadFromFileToDALDynamic() {
            MySimpleDal.Users = new List<User>();
            File.WriteAllText("users.txt", expected);

            var import = new ImportUsersFromFileDynamic();
            await import.Execute();

            Assert.Equal(5, MySimpleDal.Users.Count);
        }
    }
}
