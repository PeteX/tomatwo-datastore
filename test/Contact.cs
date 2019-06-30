using System;
using System.Collections.Generic;

namespace DataStoreTest
{
    public class Contact
    {
        public class Child
        {
            public string Name;
            public int Age;
        }

        public class Status
        {
            public int Happiness;
            public string Description;
        }

        // Use SortedDictionary so the serialisation is repeatable, which avoids spurious test failures.
        public string Id;
        public string Name;
        public List<string> PetsNames;
        public List<int> FavouriteNumbers;
        public SortedDictionary<string, int> PetsAges;
        public Child FirstChild;
        public List<Child> Children;
        public SortedDictionary<string, Status> DailyStatus;

        // Note that Firestore doesn't support lists of lists.
        public SortedDictionary<string, SortedDictionary<string, string>> DailyAttributes;
    }
}
