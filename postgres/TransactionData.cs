using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

namespace Tomatwo.DataStore.StorageServices.Postgres
{
    internal class TransactionData
    {
        internal NpgsqlConnection Connection = null;
        internal int ConnectionUsers = 0;
        internal NpgsqlTransaction Transaction = null;
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
