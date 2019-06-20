using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Tomatwo.DataStore;
using Tomatwo.DataStore.StorageServices.Firestore;

namespace DataStoreTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            IStorageService storageService = new FirestoreStorageService(new FirestoreStorageOptions {
                Project = "test-543d7",
                CredentialFile = "googleKeyfile.json",
                Prefix = "datastore"
            });

            DataStore ds = new DataStore(storageService);
            ds.AddCollection<Account>("account");

            Collection<Account> accounts = ds.GetCollection<Account>();
            string id = null;

            await ds.Transaction(async () => {
                var gates = new Account {
                    Name = "Bill Gates",
                    Gender = "Male",
                    YearOfBirth = 1955,
                    FavouriteNumber = 65536
                };

                id = await accounts.Add(gates);
                Console.WriteLine($"Attempting to add document, ID now {gates.Id}.");
            });

            Console.WriteLine($"Added document with ID {id}.");

            var may = new Account {
                Name = "Theresa May",
                Gender = "Female",
                YearOfBirth = 1956,
                FavouriteNumber = 10
            };

            await accounts.Add(may);
            Console.WriteLine($"Non-transactional add document, ID now {may.Id}.");

            var result = await accounts.Get(id);
            Console.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
        }
    }
}
