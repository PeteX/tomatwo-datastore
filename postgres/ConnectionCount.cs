using System;
using System.Threading.Tasks;
using Npgsql;

namespace Tomatwo.DataStore.StorageServices.Postgres
{
    internal sealed class ConnectionCount : IDisposable
    {
        private PostgresStorageService storageService;
        private string connectStr;
        private TransactionData td => storageService.TransactionData;

        public ConnectionCount(PostgresStorageService storageService, string connectStr)
        {
            this.storageService = storageService;
            this.connectStr = connectStr;
        }

        public async Task Open()
        {
            if (td.ConnectionUsers == 0 && td.Connection == null)
            {
                td.Connection = new NpgsqlConnection(connectStr);
                await td.Connection.OpenAsync();
            }

            td.ConnectionUsers++;
        }

        public void Dispose()
        {
            td.ConnectionUsers--;
            if(td.ConnectionUsers == 0)
            {
                td.Connection.Dispose();
                td.Connection = null;
            }
        }
    }
}
