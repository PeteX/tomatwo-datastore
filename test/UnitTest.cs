using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Tomatwo.DataStore;

namespace DataStoreTest
{
    [TestFixture]
    public class UnitTest
    {
        private Collection<Account> accounts => Setup.Accounts;

        private string canonicalise(object obj)
        {
            string json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            json = Regex.Replace(json, "^\\s*\"Id\".*$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThis\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThisToo\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "\n+", "\n");
            json = Regex.Replace(json, @",(\s*})", "$1");
            Console.WriteLine(json);
            return json;
        }

        private void verify(object result, string desired)
        {
            var firstString = canonicalise(result);
            var secondString = File.ReadAllText($"../../../results/{desired}");
            Assert.AreEqual(firstString.Trim(), secondString.Trim());
        }

        [Test]
        public async Task TestGet()
        {
            var result = await accounts.Get(Setup.Gates);
            verify(result, "TestGet.json");
        }

        [Test]
        public async Task TestQueryAllDocuments()
        {
            var result = await accounts.Query(x => true).OrderBy(x => x.Name).GetList();
            verify(result, "TestQueryAllDocuments.json");
        }

        [Test]
        public async Task TestQueryByField()
        {
            var result = await accounts.QueryList(x => x.YearOfBirth == 1955);
            result.Sort((a, b) => a.Name.CompareTo(b.Name));
            verify(result, "TestQueryByField.json");
        }

        [Test]
        public async Task TestQueryByTwoFields()
        {
            var result = await accounts.QueryList(x => x.YearOfBirth < 1956 && x.FavouriteNumber == 9 + 1);
            verify(result, "TestQueryByTwoFields.json");
        }

        [Test]
        public async Task TestWomenByYearOfBirth()
        {
            var result = await accounts.Query(x => x.Gender == "Female").OrderBy(x => x.YearOfBirth).GetList();
            verify(result, "TestWomenByYearOfBirth.json");
        }

        [Test]
        public async Task TestMenByNameDesc()
        {
            var result = await accounts.Query(x => x.Gender == "Male").OrderByDescending(x => x.Name).GetList();
            verify(result, "TestMenByNameDesc.json");
        }

        [Test]
        public async Task TestSortByTwoKeys()
        {
            var result = await accounts.Query(x => true).OrderBy(x => x.FavouriteNumber).OrderBy(x => x.Name).GetList();
            verify(result, "TestSortByTwoKeys.json");
        }

        [Test]
        public async Task TestFirstTwoByName()
        {
            var result = await accounts.Query(x => true).OrderBy(x => x.Name).Limit(2).GetList();
            verify(result, "TestFirstTwoByName.json");
        }
    }
}
