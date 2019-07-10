using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace Tomatwo.DataStore.StorageServices.Postgres
{
    public class PostgresStorageService : IStorageService
    {
        private readonly PostgresStorageOptions options;
        private AsyncLocal<TransactionData> _TransactionData = new AsyncLocal<TransactionData>();
        private static HashSet<Type> numericTypes = new HashSet<Type> {
            typeof(sbyte),
            typeof(short),
            typeof(int),
            typeof(long),

            typeof(byte),
            typeof(ushort),
            typeof(uint),
            typeof(ulong),

            typeof(float),
            typeof(double),

            typeof(decimal)
        };

        public PostgresStorageService(PostgresStorageOptions options)
        {
            this.options = options;
        }

        internal TransactionData TransactionData => _TransactionData.Value;
        private NpgsqlConnection PgConnection => TransactionData.Connection;

        // There is a subtlety here.  The AsyncLocal state propagates down the stack of async calls, but not up.  This
        // means that the TransactionData object must be set at the point of entry into PostgresStorageService.  Any
        // higher up the stack and it will get shared between different clients, any lower and it will get lost before
        // all the work is completed.
        //
        // If requireConnection is async, the value is lost immediately on return, because it is a move up the call
        // stack.  To avoid this, it is synchronous but returns a Task.

        private Task<IDisposable> requireConnection()
        {
            if (TransactionData == null)
                _TransactionData.Value = new TransactionData();

            var result = new ConnectionCount(this, options.Connect);
            return result.Open().ContinueWith(_ => (IDisposable)result);
        }

        private string getCast(Type type)
        {
            if (type == typeof(bool))
                return "boolean";

            if (type == typeof(string))
                return "text";

            if (numericTypes.Contains(type))
                return "numeric";

            throw new InvalidOperationException("Queries can only be made against boolean, numeric and string fields.");
        }

        private string getCast(MemberInfo member)
        {
            return member switch
            {
                FieldInfo field => getCast(field.FieldType),
                PropertyInfo prop => getCast(prop.PropertyType),
                _ => throw new InvalidOperationException("Queries can only be made against fields and properties.")
            };
        }

        private IDictionary<string, object> deserialise(string json)
        {
            object handleToken(JToken token) => token.Type switch
            {
                JTokenType.Object => (object)token.Children<JProperty>()
                    .ToDictionary(prop => prop.Name, prop => handleToken(prop.Value)),
                JTokenType.Array => (object)token.Select(handleToken).ToList(),
                _ => ((JValue)token).Value
            };

            return (IDictionary<string, object>)handleToken(JToken.Parse(json));
        }

        private NpgsqlCommand makeCommand(string sql)
        {
            return new NpgsqlCommand(sql, PgConnection);
        }

        public async Task<string> Add(Collection collection, IDictionary<string, object> data)
        {
            using var _ = await requireConnection();

            string id;
            if (data.TryGetValue("Id", out var idObj) && idObj != null)
            {
                id = (string)idObj;
            }
            else
            {
                id = Guid.NewGuid().ToString();
            }

            data.Remove("Id");
            using var command = makeCommand($"insert into {collection.Name} (id, data) values (@id, @data::jsonb)");
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("data", JsonConvert.SerializeObject(data));

            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (PostgresException ex)
            {
                if (ex.SqlState == "23505")
                    throw new DuplicateDocumentException($"Document {id} already exists.", ex);

                throw;
            }

            return id;
        }

        public async Task Set(Collection collection, IDictionary<string, object> data)
        {
            using var _ = await requireConnection();
            using var command = makeCommand($@"
                insert into {collection.Name} (id, data) values (@id, @data::jsonb)
                on conflict (id) do update set data = @data::jsonb");

            command.Parameters.AddWithValue("id", data["Id"]);
            data.Remove("Id");
            string doc = JsonConvert.SerializeObject(data);
            command.Parameters.AddWithValue("data", doc);
            await command.ExecuteNonQueryAsync();
        }

        public async Task<string> Update(Collection collection, string id, IReadOnlyDictionary<string, object> changes,
            bool upsert)
        {
            using var _ = await requireConnection();
            if (id == null && upsert)
                return await Add(collection, new Dictionary<string, object>(changes));

            string currentUpdate = $"{collection.Name}.data";
            var changeList = changes.ToList();

            for (int i = 0; i < changeList.Count; i++)
            {
                string fieldName = changeList[i].Key;
                currentUpdate = $"jsonb_set({currentUpdate}, '{{\"{fieldName}\"}}', @u{i}::jsonb)";
            }

            string sql;
            if (upsert)
            {
                sql = $@"
                    insert into {collection.Name} (id, data) values (@id, @data::jsonb)
                    on conflict (id) do update set data={currentUpdate}";
            }
            else
            {
                sql = $"update {collection.Name} set data={currentUpdate} where id = @id";
            }

            using var command = makeCommand(sql);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("data", JsonConvert.SerializeObject(changes));

            for (int i = 0; i < changeList.Count; i++)
                command.Parameters.AddWithValue($"u{i}", JsonConvert.SerializeObject(changeList[i].Value));

            if (await command.ExecuteNonQueryAsync() == 0)
                throw new DocumentNotFoundException($"Document {id} not found.");

            return id;
        }

        public async Task<IDictionary<string, object>> Get(Collection collection, string id)
        {
            using var _ = await requireConnection();
            using var command = makeCommand($"select data from {collection.Name} where id = @id");
            command.Parameters.AddWithValue("id", id);

            string json = (string)await command.ExecuteScalarAsync();
            if (json == null)
                return null;

            IDictionary<string, object> result = deserialise(json);
            result["Id"] = id;
            return result;
        }

        public async Task Delete(Collection collection, string id)
        {
            using var _ = await requireConnection();
            using var command = makeCommand($"delete from {collection.Name} where id = @id");
            command.Parameters.AddWithValue("id", id);
            await command.ExecuteNonQueryAsync();
        }

        public async Task<List<IDictionary<string, object>>> Query(
            Collection collection,
            IReadOnlyList<Restriction> restrictions,
            IReadOnlyList<SortKey> sortKeys,
            int limit,
            IReadOnlyList<object> startAfter)
        {
            using var _ = await requireConnection();
            StringBuilder query = new StringBuilder($"select id, data from {collection.Name}");
            List<string> sqlRestrictions = new List<string>();
            List<string> orderBy = new List<string>();
            List<string> afterAnd = new List<string>();
            List<string> afterOr = new List<string>();

            for (int i = 0; i < restrictions.Count; i++)
            {
                string sqlRestriction =
                    $"(data ->> '{restrictions[i].FieldName}')::{getCast(restrictions[i].Value.GetType())} ";

                sqlRestriction += restrictions[i].Operator switch
                {
                    ExpressionType.LessThan => "<",
                    ExpressionType.LessThanOrEqual => "<=",
                    ExpressionType.Equal => "=",
                    ExpressionType.GreaterThanOrEqual => ">=",
                    ExpressionType.GreaterThan => ">",
                    _ => throw new InvalidOperationException("Unknown query operator.")
                };

                sqlRestriction += $" @p{i}";
                sqlRestrictions.Add(sqlRestriction);
            }

            for (int i = 0; i < Math.Min(sortKeys.Count, startAfter.Count); i++)
            {
                string field = $"(data ->> '{sortKeys[i].Field.Name}')::{getCast(sortKeys[i].Field)}";
                char comparator = sortKeys[i].Ascending ? '>' : '<';
                afterAnd.Add($"{field} {comparator}= @s{i}");
                afterOr.Add($"{field} {comparator} @s{i}");
            }

            if (afterAnd.Any())
            {
                sqlRestrictions.Add("(" + string.Join(" and ", afterAnd) + ")");
                sqlRestrictions.Add("(" + string.Join(" or ", afterOr) + ")");
            }

            if (sqlRestrictions.Any())
            {
                query.Append(" where ");
                query.Append(string.Join(" and ", sqlRestrictions));
            }

            foreach (SortKey key in sortKeys)
            {
                string sort = $"(data ->> '{key.Field.Name}')::{getCast(key.Field)}";
                if (!key.Ascending)
                    sort += " desc";

                orderBy.Add(sort);
            }

            if (orderBy.Any())
            {
                query.Append(" order by ");
                query.Append(string.Join(", ", orderBy));
            }

            if (limit != 0)
                query.Append($" limit {limit}");

            using var command = makeCommand(query.ToString());

            for (int i = 0; i < restrictions.Count; i++)
                command.Parameters.AddWithValue($"p{i}", restrictions[i].Value);

            for (int i = 0; i < Math.Min(sortKeys.Count, startAfter.Count); i++)
                command.Parameters.AddWithValue($"s{i}", startAfter[i].ToString());

            using var reader = await command.ExecuteReaderAsync();
            var result = new List<IDictionary<string, object>>();
            while (await reader.ReadAsync())
            {
                string json = (string)reader[1];
                IDictionary<string, object> obj = deserialise(json);
                obj["Id"] = reader[0];
                result.Add(obj);
            }

            return result;
        }

        public async Task RunTransaction(DataStore dataStore, Func<Task> block)
        {
            for (int i = 0; ; i++)
            {
                try
                {
                    using var _ = await requireConnection();

                    {
                        TransactionData.Reset();
                        TransactionData.Transaction = PgConnection.BeginTransaction(IsolationLevel.Serializable);
                        await block();

                        foreach (var action in TransactionData.Defer)
                            action();

                        await Task.WhenAll(TransactionData.DeferAsync.Select(x => x()));
                        await TransactionData.Transaction.CommitAsync();
                    }

                    foreach (var action in TransactionData.AfterCommit)
                        action();

                    await Task.WhenAll(TransactionData.AfterCommitAsync.Select(x => x()));
                }
                catch (PostgresException ex)
                {
                    if (i < 50 /* maximum retries */ && (
                        ex.SqlState == "40P01" /* deadlock detected */ ||
                        ex.SqlState == "40001" /* serialisation failure */))
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100));
                        continue;
                    }

                    throw;
                }

                break;
            }
        }

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
