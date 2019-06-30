﻿using System.Threading.Tasks;
using NUnit.Framework;

namespace DataStoreTest
{
    [TestFixture]
    public class GeneralTests : TestBase
    {
        [Test]
        public async Task TestGet()
        {
            var result = await Accounts.Get(Setup.Gates);
            Verify(result, "TestGet.json");
        }

        [Test]
        public async Task TestQueryAllDocuments()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.Name).GetList();
            Verify(result, "TestQueryAllDocuments.json");
        }

        [Test]
        public async Task TestQueryByField()
        {
            var result = await Accounts.QueryList(x => x.YearOfBirth == 1955);
            result.Sort((a, b) => a.Name.CompareTo(b.Name));
            Verify(result, "TestQueryByField.json");
        }

        [Test]
        public async Task TestQueryByTwoFields()
        {
            var result = await Accounts.QueryList(x => x.YearOfBirth < 1956 && x.FavouriteNumber == 9 + 1);
            Verify(result, "TestQueryByTwoFields.json");
        }

        [Test]
        public async Task TestWomenByYearOfBirth()
        {
            var result = await Accounts.Query(x => x.Gender == "Female").OrderBy(x => x.YearOfBirth).GetList();
            Verify(result, "TestWomenByYearOfBirth.json");
        }

        [Test]
        public async Task TestMenByNameDesc()
        {
            var result = await Accounts.Query(x => x.Gender == "Male").OrderByDescending(x => x.Name).GetList();
            Verify(result, "TestMenByNameDesc.json");
        }

        [Test]
        public async Task TestSortByTwoKeys()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.FavouriteNumber).OrderBy(x => x.Name).GetList();
            Verify(result, "TestSortByTwoKeys.json");
        }

        [Test]
        public async Task TestFirstTwoByName()
        {
            var result = await Accounts.Query(x => true).OrderBy(x => x.Name).Limit(2).GetList();
            Verify(result, "TestFirstTwoByName.json");
        }
    }
}