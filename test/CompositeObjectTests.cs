﻿using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTests
{
    [TestFixture]
    public class CompositeObjectTests : TestBase
    {
        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            Setup();
            var existing = await Contacts.QueryList(x => true);
            await DataStore.RunTransaction(async () =>
            {
                foreach (var doc in existing)
                    await Contacts.Delete(doc.Id);
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
        public async Task TestListOfInt()
        {
            await test(new Contact
            {
                Name = "Robert Connolly",
                FavouriteNumbers = new List<int> { 2, 3, 5, 7 }
            });
        }

        [Test]
        public async Task TestListOfString()
        {
            await test(new Contact
            {
                Name = "Joe Hooper",
                PetsNames = new List<string> { "Snuffles", "Spot" }
            });
        }

        [Test]
        public async Task TestChildObject()
        {
            await test(new Contact
            {
                Name = "Ben Sutton",
                FirstChild = new Contact.Child { Name = "Emma", Age = 12 }
            });
        }

        [Test]
        public async Task TestChildObjectList()
        {
            await test(new Contact
            {
                Name = "Zoe Burrows",
                Children = new List<Contact.Child>
                {
                    new Contact.Child { Name = "Rebecca", Age = 7 },
                    new Contact.Child { Name = "Dylan", Age = 11 }
                }
            });
        }

        [Test]
        public async Task TestDictionaryWithAtomicTypes()
        {
            await test(new Contact
            {
                Name = "Joe Brooks",
                PetsAges = new SortedDictionary<string, int>
                {
                    { "Tickles", 3 },
                    { "Biscuit", 7 }
                }
            });
        }

        [Test]
        public async Task TestDictionaryWithChildObjects()
        {
            await test(new Contact
            {
                Name = "Lily Finch",
                DailyStatus = new SortedDictionary<string, Contact.Status>
                {
                    {
                        "Tuesday",
                        new Contact.Status
                        {
                            Happiness = 7,
                            Description = "Pretty happy today."
                        }
                    },
                    {
                        "Wednesday",
                        new Contact.Status
                        {
                            Happiness = 5,
                            Description = "Not so good today."
                        }
                    },
                }
            });
        }

        [Test]
        public async Task TestNestedDictionary()
        {
            await test(new Contact
            {
                Name = "Grace Wallace",
                DailyAttributes = new SortedDictionary<string, SortedDictionary<string, string>>
                {
                    {
                        "Thursday",
                        new SortedDictionary<string, string>
                        {
                            { "PersonMet", "Lily" },
                            { "Liked", "Yes" }
                        }
                    },
                    {
                        "Friday",
                        new SortedDictionary<string, string>
                        {
                            { "PersonMet", "Joe" },
                            { "Liked", "No" }
                        }
                    }
                }
            });
        }

        [Test]
        public async Task TestArrayOfInt()
        {
            await test(new Contact
            {
                Name = "Olivia Wallace",
                FavouriteNumbersArray = new int[] { 1, 4, 6, 8 }
            });
        }

        [Test]
        public async Task TestArrayOfString()
        {
            await test(new Contact
            {
                Name = "Kieran Cook",
                PetsNamesArray = new string[] { "Button", "Cleopatra" }
            });
        }

        [Test]
        public async Task TestChildObjectArray()
        {
            await test(new Contact
            {
                Name = "Christopher Doyle",
                ChildrenArray = new Contact.Child[]
                {
                    new Contact.Child { Name = "Sophia", Age = 4 },
                    new Contact.Child { Name = "Max", Age = 8 }
                }
            });
        }
    }
}
