using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTests
{
    [TestFixture]
    public class ExpressionCacheTests : TestBase
    {
        [Test]
        public async Task TestConstant()
        {
            var query = Accounts.Query(x => x.FavouriteNumber == 10);
            Assert.True(query.FastPath);
            Assert.False(query.Cacheable);
            Assert.False(query.Cached);

            var result = await query.OrderBy(x => x.Name).GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Bill Doors", result.Name);
        }

        [Test]
        public async Task TestLocal()
        {
            int number = 10;
            var query = Accounts.Query(x => x.FavouriteNumber == number);
            Assert.True(query.FastPath);
            Assert.False(query.Cacheable);
            Assert.False(query.Cached);

            var result = await query.OrderBy(x => x.Name).GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Bill Doors", result.Name);
        }

        private int numberField = 10;
        [Test]
        public async Task TestField()
        {
            var query = Accounts.Query(x => x.FavouriteNumber == numberField);
            Assert.True(query.FastPath);
            Assert.False(query.Cacheable);
            Assert.False(query.Cached);

            var result = await query.OrderBy(x => x.Name).GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Bill Doors", result.Name);
        }

        private int numberProperty { get; set; } = 10;
        [Test]
        public async Task TestProperty()
        {
            var query = Accounts.Query(x => x.FavouriteNumber == numberProperty);
            Assert.True(query.FastPath);
            Assert.False(query.Cacheable);
            Assert.False(query.Cached);

            var result = await query.OrderBy(x => x.Name).GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Bill Doors", result.Name);
        }

        [Test]
        public void TestPlus()
        {
            int local = 1;
            var result = Accounts.Query(x => x.FavouriteNumber == local + 1);
            Assert.False(result.FastPath);
            Assert.True(result.Cacheable);
            Assert.False(result.Cached);
            Assert.AreEqual("{{System.Int32}{Int32,1}+}", result.Shape.ToString());
            result = Accounts.Query(x => x.FavouriteNumber == local + 1);
            Assert.False(result.FastPath);
            Assert.True(result.Cacheable);
            Assert.True(result.Cached);
            Assert.AreEqual("{{System.Int32}{Int32,1}+}", result.Shape.ToString());
        }

        [Test]
        public void TestEscape()
        {
            string local = "Thomas";
            var result = Accounts.Query(x => x.Name == local + @" \{Funny Name\}");
            Assert.False(result.FastPath);
            Assert.True(result.Cacheable);
            Assert.False(result.Cached);
            Assert.AreEqual(@"{{System.String}{String, \\{Funny Name\\\}}+}", result.Shape.ToString());
        }

        [Test]
        public async Task TestStringArith()
        {
            string local = "Bill";
            var result = await Accounts.QuerySingleOrDefault(x => x.Name == local + " Gates");
            Assert.IsNotNull(result);
            Assert.AreEqual(1955, result.YearOfBirth);
        }

        private string append(string input) => input + " Gates";

        [Test]
        public async Task TestNonCacheable()
        {
            var query = Accounts.Query(x => x.Name == append("Bill"));
            Assert.False(query.FastPath);
            Assert.False(query.Cacheable);
            Assert.False(query.Cached);

            var result = await query.GetSingleOrDefault();
            Assert.IsNotNull(result);
            Assert.AreEqual(1955, result.YearOfBirth);
        }

        [Test]
        public async Task TestAddQuery()
        {
            int y = -1;

            var query = Accounts.Query(x => x.FavouriteNumber == y + 3);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.False(query.Cached);
            var result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);

            query = Accounts.Query(x => x.FavouriteNumber == y + 3);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.True(query.Cached);
            result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);
        }

        [Test]
        public async Task TestSubtractQuery()
        {
            int y = 1;

            var query = Accounts.Query(x => x.FavouriteNumber == 3 - y);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.False(query.Cached);
            var result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);

            query = Accounts.Query(x => x.FavouriteNumber == 3 - y);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.True(query.Cached);
            result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);
        }

        [Test]
        public async Task TestMultiplyQuery()
        {
            int y = 1;

            var query = Accounts.Query(x => x.FavouriteNumber == y * 2);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.False(query.Cached);
            var result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);

            query = Accounts.Query(x => x.FavouriteNumber == y * 2);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.True(query.Cached);
            result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);
        }

        [Test]
        public async Task TestDivideQuery()
        {
            int y = 4;

            var query = Accounts.Query(x => x.FavouriteNumber == y / 2);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.False(query.Cached);
            var result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);

            query = Accounts.Query(x => x.FavouriteNumber == y / 2);
            Assert.False(query.FastPath);
            Assert.True(query.Cacheable);
            Assert.True(query.Cached);
            result = await query.GetFirstOrDefault();
            Assert.NotNull(result);
            Assert.AreEqual("Daniel Jones", result.Name);
        }
    }
}
