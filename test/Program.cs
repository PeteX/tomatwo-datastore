using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Tomatwo.DataStore;
using Tomatwo.DataStore.StorageServices.Firestore;

namespace DataStoreTest
{
    class Program
    {
        private static DataStore ds;
        private static Collection<Account> accounts;

        private static async Task clearExisting()
        {
            var first = await accounts.QueryFirstOrDefault(x => true);
            if (first != null)
            {
                Console.WriteLine($"Deleting {first.Id}.");
                await accounts.Delete(first.Id);
            }

            await ds.Transaction(async () =>
            {
                var existing = await accounts.QueryList(x => true);
                foreach (var doc in existing)
                {
                    await accounts.Delete(doc.Id);
                }
            });
        }

        static async Task Main(string[] args)
        {
            IStorageService storageService = new FirestoreStorageService(new FirestoreStorageOptions
            {
                Project = "test-543d7",
                CredentialFile = "googleKeyfile.json",
                Prefix = "datastore"
            });

            ds = new DataStore(storageService);
            ds.AddCollection<Account>("account");

            accounts = ds.GetCollection<Account>();
            string id = null;

            await clearExisting();

            await ds.Transaction(async () =>
            {
                var gates = new Account
                {
                    Name = "Bill Gates",
                    Gender = "Male",
                    YearOfBirth = 1955,
                    FavouriteNumber = 65536
                };

                id = await accounts.Add(gates);
                Console.WriteLine($"Attempting to add document, ID now {gates.Id}.");

                await accounts.Add(new Account
                {
                    Name = "Bill Doors",
                    Gender = "Male",
                    YearOfBirth = 1955,
                    FavouriteNumber = 10
                });

                await accounts.Add(new Account
                {
                    Name = "Jack Smith",
                    Gender = "Male",
                    YearOfBirth = 1978,
                    FavouriteNumber = 729
                });

                await accounts.Add(new Account
                {
                    Name = "Daniel Jones",
                    Gender = "Male",
                    YearOfBirth = 1965,
                    FavouriteNumber = 382
                });

                await accounts.Add(new Account
                {
                    Name = "Thomas Williams",
                    Gender = "Male",
                    YearOfBirth = 1982,
                    FavouriteNumber = 699
                });

                await accounts.Add(new Account
                {
                    Name = "James Brown",
                    Gender = "Male",
                    YearOfBirth = 1990,
                    FavouriteNumber = 797
                });

                await accounts.Add(new Account
                {
                    Name = "Joshua Taylor",
                    Gender = "Male",
                    YearOfBirth = 1971,
                    FavouriteNumber = 295
                });

                await accounts.Add(new Account
                {
                    Name = "Sophie Davies",
                    Gender = "Female",
                    YearOfBirth = 1993,
                    FavouriteNumber = 238
                });

                await accounts.Add(new Account
                {
                    Name = "Chloe Wilson",
                    Gender = "Female",
                    YearOfBirth = 1995,
                    FavouriteNumber = 649
                });

                await accounts.Add(new Account
                {
                    Name = "Jessica Evans",
                    Gender = "Female",
                    YearOfBirth = 1987,
                    FavouriteNumber = 546
                });

                await accounts.Add(new Account
                {
                    Name = "Emily Thomas",
                    Gender = "Female",
                    YearOfBirth = 1965,
                    FavouriteNumber = 523
                });

                await accounts.Add(new Account
                {
                    Name = "Lauren Johnson",
                    Gender = "Female",
                    YearOfBirth = 1963,
                    FavouriteNumber = 579
                });
            });

            Console.WriteLine($"Added document with ID {id}.");

            var may = new Account
            {
                Name = "Theresa May",
                Gender = "Female",
                YearOfBirth = 1956,
                FavouriteNumber = 10
            };

            await accounts.Add(may);
            Console.WriteLine($"Non-transactional add document, ID now {may.Id}.");

            var result = await accounts.Get(id);
            Console.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));

            var results = await accounts.Query(x => true).GetList();
            Console.WriteLine("all documents\n{0}", JsonConvert.SerializeObject(results, Formatting.Indented));
            results = await accounts.QueryList(x => x.YearOfBirth == 1955);
            Console.WriteLine("born in 1955\n{0}", JsonConvert.SerializeObject(results, Formatting.Indented));
            results = await accounts.QueryList(x => x.YearOfBirth < 1956 && x.FavouriteNumber == 9 + 1);
            Console.WriteLine("born in 1955/fav nr 10\n{0}", JsonConvert.SerializeObject(results, Formatting.Indented));
        }
    }
}
