using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Tomatwo.DataStore;
using Tomatwo.DependencyInjection;

namespace DataStoreTests
{
    public class InterceptService
    {
        private int iterations = 0;

        [Inject] protected DataStore DataStore { private get; set; }

        [TransactionRequired]
        public virtual async Task<bool> TransactionTest(string gates)
        {
            Assert.True(DataStore.IsTransactionActive);

            iterations++;
            var Accounts = DataStore.GetCollection<Account>();
            var account = await Accounts.Get(gates);
            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            account.FavouriteNumber++;
            await Accounts.Set(account);

            return iterations > 1;
        }
    }
}