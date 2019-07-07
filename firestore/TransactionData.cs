using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Firestore;

namespace Tomatwo.DataStore.StorageServices.Firestore
{
    internal class TransactionData
    {
        internal Transaction Transaction;
        internal List<Action> Defer = new List<Action>();
        internal List<Func<Task>> DeferAsync = new List<Func<Task>>();
        internal List<Action> AfterCommit = new List<Action>();
        internal List<Func<Task>> AfterCommitAsync = new List<Func<Task>>();

        internal void Reset()
        {
            Defer.Clear();
            DeferAsync.Clear();
            AfterCommit.Clear();
            AfterCommitAsync.Clear();
        }
    }
}
