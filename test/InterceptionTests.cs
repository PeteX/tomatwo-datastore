using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Tomatwo.DataStore;
using Tomatwo.DependencyInjection;

namespace DataStoreTests
{
    [TestFixture]
    public class InterceptionTests : TestBase
    {
        private IServiceProvider serviceProvider;

        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            Setup();
            await ClearExisting();
            await AddTestData();

            var serviceCollection = new ServiceCollection()
                .AddSingleton<DataStore>(DataStore)
                .AddSingleton<InterceptService>();

            serviceCollection.AddEnhancedServiceProvider(provider =>
            {
                var interceptor = new TransactionInterceptor(DataStore);
                provider.AddInterceptor<TransactionRequiredAttribute>(interceptor.Interceptor);
            });

            serviceProvider = serviceCollection.BuildServiceProvider();
        }

        private async Task<bool> clash()
        {
            int iterations = 0;

            await Task.Delay(TimeSpan.FromMilliseconds(500));
            Assert.False(DataStore.IsTransactionActive);
            await DataStore.RunTransaction(async () =>
            {
                iterations++;
                Assert.True(DataStore.IsTransactionActive);
                var result = await Accounts.Get(Gates);
                result.FavouriteNumber++;
                await Accounts.Set(result);
            });

            return iterations > 1;
        }

        [Test]
        public async Task TestInterception()
        {
            Assert.False(DataStore.IsTransactionActive);

            var service = serviceProvider.GetService<InterceptService>();
            Task<bool>[] tasks = new[]{
                service.TransactionTest(Gates),
                clash()
            };

            var results = await Task.WhenAll(tasks);
            Assert.True(results[0] || results[1]);

            var final = await Accounts.Get(Gates);
            Assert.AreEqual(65538, final.FavouriteNumber);
        }
    }
}
