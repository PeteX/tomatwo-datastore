using System;
using System.Threading.Tasks;
using Tomatwo.DataStore;
using Tomatwo.DependencyInjection;

namespace DataStoreTests
{
    public class InterceptService
    {
        public static int Iterations = 0;

        [Inject] protected DataStore DataStore { private get; set; }

        [TransactionRequired]
        public virtual async Task<bool> TransactionTest(string gates)
        {
            Iterations++;
            var Accounts = DataStore.GetCollection<Account>();
            var account = await Accounts.Get(gates);
            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            account.FavouriteNumber++;
            await Accounts.Set(account);

            return account.FavouriteNumber == 65538;
        }
    }
}