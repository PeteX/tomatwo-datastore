using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        internal T MakeObject(IDictionary<string, object> input) => (T)serialiser.Deserialise(input);

        public async Task<string> Add(T document)
        {
            var data = serialiser.Serialise(document);
            string id = await DataStore.StorageService.Add(this, data);
            serialiser.SetMember(document, "Id", id);
            return id;
        }

        public async Task<T> Get(string id)
        {
            var data = await DataStore.StorageService.Get(this, id);
            var result = (T)serialiser.Deserialise(data);
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
