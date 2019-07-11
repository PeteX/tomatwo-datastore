using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTests
{
    [TestFixture]
    public class GeneralTests : TestBase
    {
        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            Setup();
            await ClearExisting();
            await AddTestData();
        }

        [Test]
        public async Task TestGet()
        {
            var result = await Accounts.Get(Gates);
            Verify(result, "TestGet.json");
        }

        [Test]
        public async Task TestGetNonexistent()
        {
            var result = await Accounts.Get("nonexistent");
            Assert.IsNull(result);
        }

        [Test]
        public async Task TestQueryAllDocuments()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.Name).GetList();
            Verify(result, "TestQueryAllDocuments.json");
        }

        [Test]
        public async Task TestQueryByField()
        {
            var result = await Accounts.QueryList(x => x.YearOfBirth == 1955);
            result.Sort((a, b) => a.Name.CompareTo(b.Name));
            Verify(result, "TestQueryByField.json");
        }

        [Test]
        public async Task TestQueryByTwoFields()
        {
            int y = 1;
            var result = await Accounts.QueryList(x => x.YearOfBirth < 1956 && x.FavouriteNumber == 9 + y);
            Verify(result, "TestQueryByTwoFields.json");
        }

        [Test]
        public async Task TestWomenByYearOfBirth()
        {
            var result = await Accounts.Query(x => x.Gender == "Female").OrderBy(x => x.YearOfBirth).GetList();
            Verify(result, "TestWomenByYearOfBirth.json");
        }

        [Test]
        public async Task TestMenByNameDesc()
        {
            var result = await Accounts.Query(x => x.Gender == "Male").OrderByDescending(x => x.Name).GetList();
            Verify(result, "TestMenByNameDesc.json");
        }

        [Test]
        public async Task TestSortByTwoKeys()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.FavouriteNumber).OrderBy(x => x.Name).GetList();
            Verify(result, "TestSortByTwoKeys.json");
        }

        [Test]
        public async Task TestFirstTwoByName()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.Name).Limit(2).GetList();
            Verify(result, "TestFirstTwoByName.json");
        }

        [Test]
        public async Task TestNextTwoByName()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.Name).Limit(2).GetList();
            result = await Accounts.Query(x => true).OrderBy(x => x.Name).StartAfter(result[1].Name).Limit(2).GetList();
            Verify(result, "TestNextTwoByName.json");
        }
    }
}
