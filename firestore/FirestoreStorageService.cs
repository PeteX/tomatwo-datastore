using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Firestore;
using Google.Cloud.Firestore.V1;
using Grpc.Auth;
using Grpc.Core;

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
            if (File.Exists(options.CredentialFile))
            {
                GoogleCredential credential = GoogleCredential.FromFile(options.CredentialFile);
                channel = new Channel(FirestoreClient.DefaultEndpoint.Host, FirestoreClient.DefaultEndpoint.Port,
                    credential.ToChannelCredentials());
            }

            FirestoreClient fc = FirestoreClient.Create(channel);
            firestoreDb = FirestoreDb.Create(options.Project, fc);
        }

        public async Task<string> Add(Collection collection, IDictionary<string, object> data)
        {
            var collRef = firestoreDb.Collection(collName(collection.Name));
            var docRef = data.ContainsKey("Id") && data["Id"] != null ? collRef.Document((string)data["Id"]) : collRef.Document();
            data.Remove("Id");//FIXME make Id configurable

            if (Transaction == null)
            {
                Console.WriteLine("Non-transactional set.");
                await docRef.SetAsync(data);
            }
            else
            {
                Console.WriteLine("Transactional set.");
                Transaction.Set(docRef, data);
            }

            return docRef.Id;
        }

        public async Task<IDictionary<string, object>> Get(Collection collection, string id)
        {
            var docRef = firestoreDb.Collection(collName(collection.Name)).Document(id);
            DocumentSnapshot result;

            if (Transaction == null)
            {
                Console.WriteLine("Non-transactional get.");
                result = await docRef.GetSnapshotAsync();
            }
            else
            {
                Console.WriteLine("Transactional get.");
                result = await Transaction.GetSnapshotAsync(docRef);
            }

            return result.ToDictionary();
        }

        public async Task<List<IDictionary<string, object>>> Query(Collection collection,
            IReadOnlyList<Restriction> restrictions, int limit)
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

            if (limit != 0)
                query = query.Limit(limit);

            QuerySnapshot snapshot;
            if (Transaction == null)
            {
                Console.WriteLine("Non-transactional query.");
                snapshot = await query.GetSnapshotAsync();
            }
            else
            {
                Console.WriteLine("Transactional query.");
                snapshot = await Transaction.GetSnapshotAsync(query);
            }

            return snapshot.Select(doc =>
            {
                IDictionary<string, object> result = doc.ToDictionary();
                result["Id"] = doc.Id;
                return result;
            }).ToList();
        }

        public async Task RunTransactionBlock(DataStore dataStore, Func<Task> block)
        {
            await firestoreDb.RunTransactionAsync(async transaction =>
            {
                Transaction = transaction;
                await block();
            });
        }
    }
}
