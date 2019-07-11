using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Firestore;
using V1 = Google.Cloud.Firestore.V1;
using Grpc.Auth;
using Grpc.Core;
using Newtonsoft.Json;

// On 11.7.19 this was benchmarked using a Google Compute Engine instance running in us-central1, which is the same
// region as the database.
//
// Simple reads started out taking about 100ms but this quickly came down to about 24ms, so presumably the slow start
// was because of the code being JITed.  (The connection was already established because the program started by setting
// up test data.)
//
// Simple writes also started out at around 100ms but these only came down to about 57ms.
//
// Variation between test runs seems to be about 50%.

namespace Tomatwo.DataStore.StorageServices.Firestore
{
    public class FirestoreStorageService : IStorageService
    {
        private readonly FirestoreStorageOptions options;
        private readonly FirestoreDb firestoreDb;
        private AsyncLocal<TransactionData> _Transaction = new AsyncLocal<TransactionData>();
        private string collName(string name) => (options.Prefix == null ? "" : $"{options.Prefix}.") + name;

        private TransactionData TransactionData
        {
            get => _Transaction.Value;
            set => _Transaction.Value = value;
        }

        private Transaction FsTransaction
        {
            get => TransactionData?.Transaction;
            set => TransactionData.Transaction = value;
        }

        public FirestoreStorageService(FirestoreStorageOptions options)
        {
            this.options = options;

            Channel channel = null;
            string project = null;
            if (File.Exists(options.CredentialFile))
            {
                GoogleCredential credential = GoogleCredential.FromFile(options.CredentialFile);
                channel = new Channel(V1.FirestoreClient.DefaultEndpoint.Host, V1.FirestoreClient.DefaultEndpoint.Port,
                    credential.ToChannelCredentials());

                project = JsonConvert.DeserializeObject<Dictionary<string, string>>(
                    File.ReadAllText(options.CredentialFile))["project_id"];
            }

            V1.FirestoreClient fc = V1.FirestoreClient.Create(channel);
            firestoreDb = FirestoreDb.Create(project, fc);
        }

        public async Task<string> Add(Collection collection, IDictionary<string, object> data)
        {
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = data.ContainsKey("Id") && data["Id"] != null ?
                collRef.Document((string)data["Id"]) : collRef.Document();
            data.Remove("Id");

            if (FsTransaction == null)
            {
                try
                {
                    await docRef.CreateAsync(data);
                }
                catch (RpcException ex)
                {
                    if (ex.StatusCode == StatusCode.AlreadyExists)
                        throw new DuplicateDocumentException($"Document {docRef.Id} already exists.", ex);

                    throw;
                }
            }
            else
            {
                FsTransaction.Create(docRef, data);
            }

            return docRef.Id;
        }

        public async Task Set(Collection collection, IDictionary<string, object> data)
        {
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = collRef.Document((string)data["Id"]);
            data.Remove("Id");

            if (FsTransaction == null)
            {
                await docRef.SetAsync(data);
            }
            else
            {
                FsTransaction.Set(docRef, data);
            }
        }

        public async Task<string> Update(Collection collection, string id, IReadOnlyDictionary<string, object> changes,
            bool upsert)
        {
            var copyChanges = new Dictionary<string, object>(changes);
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = id != null ? collRef.Document(id) : collRef.Document();
            var precondition = upsert ? Precondition.None : null;

            if (FsTransaction == null)
            {
                try
                {
                    await docRef.UpdateAsync(copyChanges, precondition);
                }
                catch (RpcException ex)
                {
                    if (ex.StatusCode == StatusCode.NotFound)
                        throw new DocumentNotFoundException($"Document {docRef.Id} not found.", ex);

                    throw;
                }
            }
            else
            {
                FsTransaction.Update(docRef, copyChanges, precondition);
            }

            return docRef.Id;
        }

        public async Task<IDictionary<string, object>> Get(Collection collection, string id)
        {
            var docRef = firestoreDb.Collection(collName(collection.Name)).Document(id);
            DocumentSnapshot result;

            if (FsTransaction == null)
            {
                result = await docRef.GetSnapshotAsync();
            }
            else
            {
                result = await FsTransaction.GetSnapshotAsync(docRef);
            }

            if (!result.Exists)
                return null;

            return result.ToDictionary();
        }

        public async Task Delete(Collection collection, string id)
        {
            var docRef = firestoreDb.Collection(collName(collection.Name)).Document(id);

            if (FsTransaction == null)
            {
                await docRef.DeleteAsync();
            }
            else
            {
                FsTransaction.Delete(docRef);
            }
        }

        public async Task<List<IDictionary<string, object>>> Query(
            Collection collection,
            IReadOnlyList<Restriction> restrictions,
            IReadOnlyList<SortKey> sortKeys,
            int limit,
            IReadOnlyList<object> startAfter)
        {
            var collRef = firestoreDb.Collection(collName(collection.Name));
            Query query = collRef;

            foreach (var restriction in restrictions)
            {
                query = restriction.Operator switch
                {
                    ExpressionType.LessThan =>
                        query.WhereLessThan(restriction.FieldName, restriction.Value),
                    ExpressionType.LessThanOrEqual =>
                        query.WhereLessThanOrEqualTo(restriction.FieldName, restriction.Value),
                    ExpressionType.Equal =>
                        query.WhereEqualTo(restriction.FieldName, restriction.Value),
                    ExpressionType.GreaterThanOrEqual =>
                        query.WhereGreaterThanOrEqualTo(restriction.FieldName, restriction.Value),
                    ExpressionType.GreaterThan =>
                        query.WhereGreaterThan(restriction.FieldName, restriction.Value),
                    _ => throw new InvalidOperationException("Unknown query operator.")
                };
            }

            foreach (SortKey key in sortKeys)
            {
                if (key.Ascending)
                {
                    query = query.OrderBy(key.Field.Name);
                }
                else
                {
                    query = query.OrderByDescending(key.Field.Name);
                }
            }

            if (limit != 0)
                query = query.Limit(limit);

            foreach (object start in startAfter)
                query = query.StartAfter(start);

            QuerySnapshot snapshot;
            if (FsTransaction == null)
            {
                snapshot = await query.GetSnapshotAsync();
            }
            else
            {
                snapshot = await FsTransaction.GetSnapshotAsync(query);
            }

            return snapshot.Select(doc =>
            {
                IDictionary<string, object> result = doc.ToDictionary();
                result["Id"] = doc.Id;
                return result;
            }).ToList();
        }

        public async Task RunTransaction(DataStore dataStore, Func<Task> block)
        {
            try
            {
                TransactionData = new TransactionData();
                await firestoreDb.RunTransactionAsync(async transaction =>
                {
                    TransactionData.Reset();
                    FsTransaction = transaction;
                    await block();

                    foreach (var action in TransactionData.Defer)
                        action();

                    await Task.WhenAll(TransactionData.DeferAsync.Select(x => x()));
                });

                foreach (var action in TransactionData.AfterCommit)
                    action();

                await Task.WhenAll(TransactionData.AfterCommitAsync.Select(x => x()));
            }
            catch (RpcException ex)
            {
                if (ex.StatusCode == StatusCode.AlreadyExists)
                    throw new DuplicateDocumentException("Document already exists.", ex);
                if (ex.StatusCode == StatusCode.NotFound)
                    throw new DocumentNotFoundException("Document not found.", ex);

                throw;
            }
            finally
            {
                TransactionData = null;
            }
        }

        public bool IsTransactionActive => TransactionData != null;

        public void Defer(Action action)
        {
            TransactionData.Defer.Add(action);
        }

        public void DeferAsync(Func<Task> action)
        {
            TransactionData.DeferAsync.Add(action);
        }

        public void AfterCommit(Action action)
        {
            TransactionData.AfterCommit.Add(action);
        }

        public void AfterCommitAsync(Func<Task> action)
        {
            TransactionData.AfterCommitAsync.Add(action);
        }
    }
}
