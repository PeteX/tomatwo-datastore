using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Tomatwo.DataStore;
using Tomatwo.DataStore.StorageServices.Firestore;
using Tomatwo.DataStore.StorageServices.Postgres;

namespace DataStoreTests
{
    public class TestBase
    {
        protected static DataStore DataStore = null;
        protected static Collection<Account> Accounts;
        protected static Collection<Contact> Contacts;
        protected static string Gates;

        protected string Canonicalise(object obj)
        {
            string json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            json = Regex.Replace(json, "^\\s*\"Id\".*$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThis\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThisToo\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "\n+", "\n");
            json = Regex.Replace(json, @",(\s*})", "$1");
            return json;
        }

        protected void Verify(object result, string desired)
        {
            var resultString = Canonicalise(result);
            var desiredString = File.ReadAllText($"../../../results/{desired}");
            Assert.AreEqual(desiredString.Trim(), resultString.Trim());
        }

        protected void Setup()
        {
            if (DataStore == null)
            {
                string backend = File.ReadAllText("../../../backend.txt").Trim();
                IStorageService storageService;

                if (backend == "postgres")
                {
                    storageService = new PostgresStorageService(new PostgresStorageOptions
                    {
                        Connect = "Server=/run/postgresql; Port=5432; Database=postgres; User Id=datastoretest"
                    });
                }
                else
                {
                    storageService = new FirestoreStorageService(new FirestoreStorageOptions
                    {
                        CredentialFile = "../../../../googleKeyfile.json",
                        Prefix = "datastore"
                    });
                }

                DataStore = new DataStore(storageService);
                Accounts = DataStore.AddCollection<Account>("account");
                DataStore.AddCollection<Contact>("contact");
                Contacts = DataStore.GetCollection<Contact>();
            }
        }

        protected async Task ClearExisting()
        {
            var first = await Accounts.QueryFirstOrDefault(x => true);
            if (first != null)
            {
                await Accounts.Delete(first.Id);
            }

            await DataStore.RunTransaction(async () =>
            {
                var existing = await Accounts.QueryList(x => true);
                foreach (var doc in existing)
                {
                    await Accounts.Delete(doc.Id);
                }
            });
        }

        protected async Task AddTestData()
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

            await DataStore.RunTransaction(async () =>
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
    }
}
