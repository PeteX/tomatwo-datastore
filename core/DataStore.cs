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

        public Collection<T> AddCollection<T>(string name) where T : new()
        {
            var collection = new Collection<T>(this, name);
            collections[typeof(T)] = collection;
            return collection;
        }

        public Collection<T> GetCollection<T>() where T : new() => (Collection<T>)collections[typeof(T)];

        public async Task RunTransaction(Func<Task> block)
        {
            await StorageService.RunTransaction(this, block);
        }

        public void Defer(Action action) => StorageService.Defer(action);
        public void DeferAsync(Func<Task> action) => StorageService.DeferAsync(action);
        public void AfterCommit(Action action) => StorageService.AfterCommit(action);
        public void AfterCommitAsync(Func<Task> action) => StorageService.AfterCommitAsync(action);
    }
}
