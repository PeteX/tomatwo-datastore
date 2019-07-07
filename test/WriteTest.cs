using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using Tomatwo.DataStore;

namespace DataStoreTests
{
    [TestFixture]
    public class WriteTest : TestBase
    {
        private Account testAccount => new Account
        {
            Name = "Faith Cunningham",
            Gender = "Female",
            YearOfBirth = 1938,
            FavouriteNumber = 5
        };

        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            Setup();
            await ClearExisting();
        }

        [Test]
        public async Task TestCreateRandomId()
        {
            var id = await Accounts.Add(testAccount);
            var retrieved = await Accounts.QuerySingle(x => x.Name == "Faith Cunningham");
            Assert.AreEqual(id, retrieved.Id);
            Assert.IsNotNull(id);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestCreateProvidedId()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual(account.YearOfBirth, retrieved.YearOfBirth);
            Assert.AreEqual("Faith Cunningham", id, account.Id, retrieved.Id);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestCreateDoesntOverwrite()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            Assert.ThrowsAsync<DuplicateDocumentException>(async () =>
            {
                account.YearOfBirth = 1939;
                await Accounts.Add(account);
            });

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual(1938, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestTransactionalCreateDoesntOverwrite()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            Assert.ThrowsAsync<DuplicateDocumentException>(async () =>
            {
                await DataStore.RunTransaction(async () =>
                {
                    account.YearOfBirth = 1939;
                    await Accounts.Add(account);
                });
            });

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual(1938, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestSet()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            account.YearOfBirth = 1939;
            await Accounts.Set(account);

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual(1939, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestUpdate()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            await Accounts.Update("Faith Cunningham", new Dictionary<string, object> { { "YearOfBirth", 1941 } });

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual(1941, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestUpsertWithExisting()
        {
            var account = testAccount;
            account.Id = "Faith Cunningham";
            var id = await Accounts.Add(account);

            var id2 = await Accounts.Update(
                "Faith Cunningham", new Dictionary<string, object> { { "YearOfBirth", 1942 } }, upsert: true);

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual("Faith Cunningham", id, id2);
            Assert.AreEqual(1942, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestUpsertWithoutExisting()
        {
            var account = new Dictionary<string, object>
            {
                { "Name", "Faith Cunningham" },
                { "YearOfBirth", 1943 }
            };

            var id = await Accounts.Update("Faith Cunningham", account, upsert: true);

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.AreEqual("Faith Cunningham", id, retrieved.Id, retrieved.Name);
            Assert.AreEqual(1943, retrieved.YearOfBirth);
            await Accounts.Delete(id);
        }

        [Test]
        public async Task TestDisabledUpsert()
        {
            var account = new Dictionary<string, object>
            {
                { "Name", "Faith Cunningham" },
                { "YearOfBirth", 1943 }
            };

            Assert.ThrowsAsync<DocumentNotFoundException>(async () =>
                await Accounts.Update("Faith Cunningham", account));

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.IsNull(retrieved);
            retrieved = await Accounts.QuerySingleOrDefault(x => x.YearOfBirth == 1943);
            Assert.IsNull(retrieved);
        }

        [Test]
        public async Task TestDisabledTransactionalUpsert()
        {
            var account = new Dictionary<string, object>
            {
                { "Name", "Faith Cunningham" },
                { "YearOfBirth", 1943 }
            };

            Assert.ThrowsAsync<DocumentNotFoundException>(async () =>
            {
                await DataStore.RunTransaction(async () =>
                {
                    await Accounts.Update("Faith Cunningham", account);
                });
            });

            var retrieved = await Accounts.Get("Faith Cunningham");
            Assert.IsNull(retrieved);
            retrieved = await Accounts.QuerySingleOrDefault(x => x.YearOfBirth == 1943);
            Assert.IsNull(retrieved);
        }
    }
}
