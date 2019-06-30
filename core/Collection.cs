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

        private Dictionary<string, Func<T, object>> getters = new Dictionary<string, Func<T, object>>();
        private Dictionary<string, Action<T, object>> setters = new Dictionary<string, Action<T, object>>();

        private static TOuter makeListType<TOuter, TInner>(object input) where TOuter : IList<TInner>, new()
        {
            if (input == null)
                return default(TOuter);

            TOuter result = new TOuter();

            foreach (object obj in (IList<object>)input)
            {
                result.Add((TInner)Convert.ChangeType(obj, typeof(TInner)));
            }

            return result;
        }

        private Expression setConverter(Expression value, Type memberType)
        {
            MethodInfo changeType =
                typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(object), typeof(Type) });

            if (memberType.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>)))
            {
                Type listContent = memberType.GetGenericArguments()[0];
                MethodInfo method = GetType().GetMethod("makeListType", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(memberType, listContent);
                return Expression.Call(null, specialised, value);
            }
            else
            {
                Expression propertyType = Expression.Constant(memberType);
                value = Expression.Call(changeType, value, propertyType);
                return Expression.Convert(value, memberType);
            }
        }

        internal Collection(DataStore dataStore, string name) : base(name)
        {
            this.DataStore = dataStore;

            foreach (MemberInfo member in typeof(T).GetMembers())
            {
                PropertyInfo prop = member as PropertyInfo;
                FieldInfo field = member as FieldInfo;

                if ((prop != null || field != null) && member.GetCustomAttribute<DsIgnoreAttribute>() == null)
                {
                    Type memberType = prop != null ? prop.PropertyType : field.FieldType;
                    ParameterExpression obj = Expression.Parameter(typeof(T));
                    Expression getter = prop != null ?
                        (Expression)Expression.Call(obj, prop.GetMethod) :
                        (Expression)Expression.Field(obj, field);
                    Expression box = Expression.Convert(getter, typeof(object));
                    getters[member.Name] = Expression.Lambda<Func<T, object>>(box, obj).Compile();

                    ParameterExpression valueObj = Expression.Parameter(typeof(object));
                    Expression value = setConverter(valueObj, memberType);
                    Expression setter = prop != null ?
                        (Expression)Expression.Call(obj, prop.SetMethod, value) :
                        (Expression)Expression.Assign(Expression.Field(obj, field), value);
                    setters[member.Name] = Expression.Lambda<Action<T, object>>(setter, obj, valueObj).Compile();
                }
            }
        }

        public async Task<string> Add(T document)
        {
            var data = new Dictionary<string, object>();
            foreach ((string name, var getter) in getters)
            {
                data[name] = getter(document);
            }

            string id = await DataStore.StorageService.Add(this, data);
            this.setters["Id"](document, id);
            return id;
        }

        internal T MakeObject(IDictionary<string, object> data)
        {
            T result = new T();
            foreach ((string name, object value) in data)
            {
                setters[name](result, value);
            }

            return result;
        }

        public async Task<T> Get(string id)
        {
            var data = await DataStore.StorageService.Get(this, id);
            var result = MakeObject(data);
            setters["Id"](result, id);
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
