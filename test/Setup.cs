using System.Threading.Tasks;
using NUnit.Framework;
using Tomatwo.DataStore;
using Tomatwo.DataStore.StorageServices.Firestore;

namespace DataStoreTest
{
    [SetUpFixture]
    public class Setup
    {
        public static DataStore DataStore;
        public static Collection<Account> Accounts;
        public static Collection<Contact> Contacts;
        public static string Gates;

        private async Task clearExisting()
        {
            var first = await Accounts.QueryFirstOrDefault(x => true);
            if (first != null)
            {
                System.Console.WriteLine($"Deleting {first.Id}.");
                await Accounts.Delete(first.Id);
            }

            await DataStore.Transaction(async () =>
            {
                var existing = await Accounts.QueryList(x => true);
                foreach (var doc in existing)
                {
                    await Accounts.Delete(doc.Id);
                }
            });
        }

        private async Task addTestData()
        {
            Gates = await Accounts.Add(new Account
            {
                Name = "Bill Gates",
                Gender = "Male",
                YearOfBirth = 1955,
                FavouriteNumber = 65536,
                IgnoreThis = "This should not be stored in the database.",
                IgnoreThisToo = "Nor should this."
            });

            await Accounts.Add(new Account
            {
                Name = "Bill Doors",
                Gender = "Male",
                YearOfBirth = 1955,
                FavouriteNumber = 10
            });

            await DataStore.Transaction(async () =>
            {
                await Accounts.Add(new Account
                {
                    Name = "Theresa May",
                    Gender = "Female",
                    YearOfBirth = 1956,
                    FavouriteNumber = 10
                });

                await Accounts.Add(new Account
                {
                    Name = "Jack Smith",
                    Gender = "Male",
                    YearOfBirth = 1978,
                    FavouriteNumber = 9
                });

                await Accounts.Add(new Account
                {
                    Name = "Daniel Jones",
                    Gender = "Male",
                    YearOfBirth = 1965,
                    FavouriteNumber = 2
                });

                await Accounts.Add(new Account
                {
                    Name = "Thomas Williams",
                    Gender = "Male",
                    YearOfBirth = 1982,
                    FavouriteNumber = 9
                });

                await Accounts.Add(new Account
                {
                    Name = "James Brown",
                    Gender = "Male",
                    YearOfBirth = 1990,
                    FavouriteNumber = 7
                });

                await Accounts.Add(new Account
                {
                    Name = "Joshua Taylor",
                    Gender = "Male",
                    YearOfBirth = 1971,
                    FavouriteNumber = 5
                });

                await Accounts.Add(new Account
                {
                    Name = "Sophie Davies",
                    Gender = "Female",
                    YearOfBirth = 1993,
                    FavouriteNumber = 8
                });

                await Accounts.Add(new Account
                {
                    Name = "Chloe Wilson",
                    Gender = "Female",
                    YearOfBirth = 1995,
                    FavouriteNumber = 9
                });

                await Accounts.Add(new Account
                {
                    Name = "Jessica Evans",
                    Gender = "Female",
                    YearOfBirth = 1987,
                    FavouriteNumber = 6
                });

                await Accounts.Add(new Account
                {
                    Name = "Emily Thomas",
                    Gender = "Female",
                    YearOfBirth = 1965,
                    FavouriteNumber = 3
                });

                await Accounts.Add(new Account
                {
                    Name = "Lauren Johnson",
                    Gender = "Female",
                    YearOfBirth = 1963,
                    FavouriteNumber = 9
                });
            });
        }

        [OneTimeSetUp]
        public async Task RunBeforeAnyTests()
        {
            IStorageService storageService = new FirestoreStorageService(new FirestoreStorageOptions
            {
                CredentialFile = "../../../../googleKeyfile.json",
                Prefix = "datastore"
            });

            DataStore = new DataStore(storageService);
            DataStore.AddCollection<Account>("account");
            DataStore.AddCollection<Contact>("contact");
            Accounts = DataStore.GetCollection<Account>();
            Contacts = DataStore.GetCollection<Contact>();
            await clearExisting();
            await addTestData();
        }
    }
}
