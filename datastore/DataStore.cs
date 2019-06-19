using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tomatwo.DataStore
{
    public class DataStore
    {
        public IStorageService StorageService { get; }

        private Dictionary<Type, Collection> collections = new Dictionary<Type, Collection>();

        public DataStore(IStorageService storageService)
        {
            this.StorageService = storageService;
        }

        public void AddCollection<T>(string name)
        {
            collections[typeof(T)] = new Collection<T>(this, name);
        }

        public Collection<T> GetCollection<T>() => (Collection<T>) collections[typeof(T)];

        public async Task Transaction(Func<Task> block)
        {
            await StorageService.RunTransactionBlock(this, block);
        }
    }
}
