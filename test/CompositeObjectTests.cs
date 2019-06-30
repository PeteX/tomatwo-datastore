using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTest
{
    [TestFixture]
    public class CompositeObjectTests : TestBase
    {
        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            var existing = await Contacts.QueryList(x => true);
            await DataStore.Transaction(async () =>
            {
                foreach (var doc in existing)
                {
                    await Contacts.Delete(doc.Id);
                }
            });
        }

        private async Task test(Contact contact)
        {
            string id = await Contacts.Add(contact);
            var retrieved = await Contacts.Get(id);
            Assert.AreEqual(Canonicalise(contact), Canonicalise(retrieved));
        }

        [Test]
        public async Task TestSimple()
        {
            await test(new Contact
            {
                Name = "Luke Marsh"
            });
        }

        [Test]
        public async Task TestListOfString()
        {
            await test(new Contact
            {
                Name = "Luke Marsh",
                PetsNames = new List<string> { "Snuffles", "Spot" }
            });
        }
    }
}
