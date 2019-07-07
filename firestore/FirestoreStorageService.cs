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

namespace Tomatwo.DataStore.StorageServices.Firestore
{
    public class FirestoreStorageService : IStorageService
    {
        public Transaction Transaction
        {
            get => _Transaction.Value;
            private set => _Transaction.Value = value;
        }

        private readonly FirestoreStorageOptions options;
        private readonly FirestoreDb firestoreDb;
        private AsyncLocal<Transaction> _Transaction = new AsyncLocal<Transaction>();
        private string collName(string name) => (options.Prefix == null ? "" : $"{options.Prefix}.") + name;

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

            if (Transaction == null)
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
                Transaction.Create(docRef, data);
            }

            return docRef.Id;
        }

        public async Task Set(Collection collection, IDictionary<string, object> data)
        {
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = collRef.Document((string)data["Id"]);
            data.Remove("Id");

            if (Transaction == null)
            {
                await docRef.SetAsync(data);
            }
            else
            {
                Transaction.Set(docRef, data);
            }
        }

        public async Task<string> Update(Collection collection, string id, IReadOnlyDictionary<string, object> changes,
            bool upsert)
        {
            var copyChanges = new Dictionary<string, object>(changes);
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = id != null ? collRef.Document(id) : collRef.Document();
            var precondition = upsert ? Precondition.None : null;

            if (Transaction == null)
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
                Transaction.Update(docRef, copyChanges, precondition);
            }

            return docRef.Id;
        }

        public async Task<IDictionary<string, object>> Get(Collection collection, string id)
        {
            var docRef = firestoreDb.Collection(collName(collection.Name)).Document(id);
            DocumentSnapshot result;

            if (Transaction == null)
            {
                result = await docRef.GetSnapshotAsync();
            }
            else
            {
                result = await Transaction.GetSnapshotAsync(docRef);
            }

            if (!result.Exists)
                return null;

            return result.ToDictionary();
        }

        public async Task Delete(Collection collection, string id)
        {
            var docRef = firestoreDb.Collection(collName(collection.Name)).Document(id);

            if (Transaction == null)
            {
                await docRef.DeleteAsync();
            }
            else
            {
                Transaction.Delete(docRef);
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
                    query = query.OrderBy(key.FieldName);
                }
                else
                {
                    query = query.OrderByDescending(key.FieldName);
                }
            }

            if (limit != 0)
                query = query.Limit(limit);

            foreach(object start in startAfter)
                query = query.StartAfter(start);

            QuerySnapshot snapshot;
            if (Transaction == null)
            {
                snapshot = await query.GetSnapshotAsync();
            }
            else
            {
                snapshot = await Transaction.GetSnapshotAsync(query);
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
                await firestoreDb.RunTransactionAsync(async transaction =>
                {
                    Transaction = transaction;
                    await block();
                });
            }
            catch (RpcException ex)
            {
                if (ex.StatusCode == StatusCode.AlreadyExists)
                    throw new DuplicateDocumentException("Document already exists.", ex);
                if (ex.StatusCode == StatusCode.NotFound)
                    throw new DocumentNotFoundException("Document not found.", ex);

                throw;
            }
        }
    }
}
