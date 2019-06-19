using Tomatwo.DataStore;

namespace DataStoreTest
{
    public class Account
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Gender;
        public int YearOfBirth;

        [DsIgnore] public string IgnoreThis { get; set; } = "ignore";
        [DsIgnore] public string IgnoreThisToo = "ignore";
    }
}
