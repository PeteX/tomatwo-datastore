using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTest
{
    [TestFixture]
    public class TransactionTest : TestBase
    {
        private int iterations = 0;
        private Account saved;

        public async Task AddOne()
        {
            await DataStore.Transaction(async () =>
            {
                Interlocked.Increment(ref iterations);
                var person = await Accounts.QuerySingle(x => x.Name == "Bill Gates");
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                await Accounts.Update(
                    person.Id,
                    new Dictionary<string, object> { { "FavouriteNumber", person.FavouriteNumber + 1 } },
                    false);
            });
        }

        [Test]
        public async Task TestTransactions()
        {
            var start = await Accounts.QuerySingle(x => x.Name == "Bill Gates");

            Task[] tasks = new Task[3];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = AddOne();
            }

            await Task.WhenAll(tasks);
            Assert.Greater(iterations, tasks.Length);

            var end = await Accounts.QuerySingle(x => x.Name == "Bill Gates");
            Assert.AreEqual(start.FavouriteNumber + tasks.Length, end.FavouriteNumber);
        }

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            saved = await Accounts.QuerySingle(x => x.Name == "Bill Gates");
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await Accounts.Set(saved);
        }
    }
}
