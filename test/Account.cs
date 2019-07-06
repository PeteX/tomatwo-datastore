using Tomatwo.DataStore;

namespace DataStoreTests
{
    public class Account
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Gender;
        public int YearOfBirth;
        public int FavouriteNumber { get; set; }

        [DsIgnore] public string IgnoreThis { get; set; } = "ignore";
        [DsIgnore] public string IgnoreThisToo = "ignore";
    }
}
