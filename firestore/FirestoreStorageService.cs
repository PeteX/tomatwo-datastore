using System;
using System.Collections.Generic;
using System.IO;
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
        public Transaction Transaction {
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
            if(File.Exists(options.CredentialFile))
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
            var docRef = data.ContainsKey("Id") && data["Id"] != null ? collRef.Document((string) data["Id"]) : collRef.Document();
            data.Remove("Id");//FIXME make Id configurable

            if(Transaction == null)
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

            if(Transaction == null)
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
