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

        public string Id;
        public string Name;
        public List<string> PetsNames;
        public Dictionary<string, int> PetsAges;
        public Child FirstChild;
        public List<Child> Children;
        public Dictionary<DateTime, Status> DailyStatus;
    }
}
