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
            var result = await Accounts.Get(Gates);
            result.FavouriteNumber++;
            await Accounts.Set(result);

            // The return value is not important, but making it the same as TransactionTest makes it easier to handle
            // the results.

            return true;
        }

        [Test]
        public async Task TestInterception()
        {
            var service = serviceProvider.GetService<InterceptService>();
            Task<bool>[] tasks = new[]{
                service.TransactionTest(Gates),
                clash()
            };

            var results = await Task.WhenAll(tasks);
            Assert.True(results[0]);
            Assert.True(results[1]);
            Assert.Greater(InterceptService.Iterations, 1);
        }
    }
}
