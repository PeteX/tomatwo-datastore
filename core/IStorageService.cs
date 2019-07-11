using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tomatwo.DataStore
{
    public interface IStorageService
    {
        Task RunTransaction(DataStore dataStore, Func<Task> block);
        bool IsTransactionActive { get; }
        void Defer(Action action);
        void DeferAsync(Func<Task> action);
        void AfterCommit(Action action);
        void AfterCommitAsync(Func<Task> action);

        Task<string> Add(Collection collection, IDictionary<string, object> data);
        Task Set(Collection collection, IDictionary<string, object> data);
        Task<string> Update(Collection collection, string id, IReadOnlyDictionary<string, object> changes, bool upsert);
        Task<IDictionary<string, object>> Get(Collection collection, string id);
        Task Delete(Collection collection, string id);

        Task<List<IDictionary<string, object>>> Query(
            Collection collection,
            IReadOnlyList<Restriction> restrictions,
            IReadOnlyList<SortKey> sortKeys,
            int limit,
            IReadOnlyList<object> startAfter);
    }
}
