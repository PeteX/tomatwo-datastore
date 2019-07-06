using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTests
{
    [TestFixture]
    public class ExpressionCacheTests : TestBase
    {
        [Test]
        public void TestConstant()
        {
            var result = Accounts.Query(x => x.FavouriteNumber == 10);
            Assert.True(result.FastPath);
            Assert.False(result.Cacheable);
            Assert.False(result.Cached);
        }

        [Test]
        public void TestLocal()
        {
            int number = 10;
            var result = Accounts.Query(x => x.FavouriteNumber == number);
            Assert.True(result.FastPath);
            Assert.False(result.Cacheable);
            Assert.False(result.Cached);
        }

        private int numberField = 10;
        [Test]
        public void TestField()
        {
            var result = Accounts.Query(x => x.FavouriteNumber == numberField);
            Assert.True(result.FastPath);
            Assert.False(result.Cacheable);
            Assert.False(result.Cached);
        }

        private int numberProperty { get; set; } = 10;
        [Test]
        public void TestProperty()
        {
            var result = Accounts.Query(x => x.FavouriteNumber == numberProperty);
            Assert.True(result.FastPath);
            Assert.False(result.Cacheable);
            Assert.False(result.Cached);
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
    }
}
