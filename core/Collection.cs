using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Tomatwo.DataStore
{
    public class Collection
    {
        public string Name { get; private set; }

        public Collection(string name)
        {
            this.Name = name;
        }
    }

    public class Collection<T> : Collection where T : new()
    {
        public DataStore DataStore { get; private set; }

        private ObjectSerialiser serialiser;

        internal Collection(DataStore dataStore, string name) : base(name)
        {
            this.DataStore = dataStore;
            serialiser = ObjectSerialiser.GetSerialiser<T>();
        }

        internal T Deserialise(IDictionary<string, object> input) => (T)serialiser.Deserialise(input);
        internal IDictionary<string, object> Serialise(T obj) => serialiser.Serialise(obj);

        public async Task<string> Add(T document)
        {
            var data = Serialise(document);
            string id = await DataStore.StorageService.Add(this, data);
            serialiser.SetMember(document, "Id", id);
            return id;
        }

        public async Task<string> Set(T document)
        {
            var data = Serialise(document);
            if (!data.TryGetValue("Id", out var id) || id == null)
                throw new InvalidOperationException("The document Id field must be filled in when calling Set.");

            await DataStore.StorageService.Set(this, data);
            return (string)id;
        }

        public Task<string> Update(string id, Dictionary<string, object> changes, bool upsert = false) =>
            DataStore.StorageService.Update(this, id, changes, upsert);

        public async Task<T> Get(string id)
        {
            var data = await DataStore.StorageService.Get(this, id);

            if(data == null)
                return default(T);

            var result = Deserialise(data);
            serialiser.SetMember(result, "Id", id);
            return result;
        }

        public Task Delete(string id) => DataStore.StorageService.Delete(this, id);

        public Task<T> QueryFirst(Expression<Func<T, bool>> select) => Query(select).GetFirst();
        public Task<T> QueryFirstOrDefault(Expression<Func<T, bool>> select) => Query(select).GetFirstOrDefault();
        public Task<T> QuerySingle(Expression<Func<T, bool>> select) => Query(select).GetSingle();
        public Task<T> QuerySingleOrDefault(Expression<Func<T, bool>> select) => Query(select).GetSingleOrDefault();
        public Task<List<T>> QueryList(Expression<Func<T, bool>> select) => Query(select).GetList();
        public Query<T> Query(Expression<Func<T, bool>> select) => new Query<T>(this, select);
    }
}
