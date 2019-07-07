using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTests
{
    [TestFixture]
    public class TransactionTest : TestBase
    {
        private int iterations = 0;
        private int commits = 0;
        private int asyncTest = 0;

        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            Setup();
            await ClearExisting();
            await AddTestData();
        }

        public async Task AddOne()
        {
            await DataStore.RunTransaction(async () =>
            {
                DataStore.Defer(() => Interlocked.Increment(ref iterations));
                DataStore.AfterCommit(() => Interlocked.Increment(ref commits));

                DataStore.DeferAsync(() =>
                {
                    Interlocked.Increment(ref asyncTest);
                    return Task.CompletedTask;
                });

                DataStore.AfterCommitAsync(() =>
                {
                    Interlocked.Increment(ref asyncTest);
                    return Task.CompletedTask;
                });

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
            Assert.Less(tasks.Length, iterations);
            Assert.AreEqual(tasks.Length, commits);
            Assert.AreEqual(iterations + commits, asyncTest);

            var end = await Accounts.QuerySingle(x => x.Name == "Bill Gates");
            Assert.AreEqual(start.FavouriteNumber + tasks.Length, end.FavouriteNumber);
        }
    }
}
