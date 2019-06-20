using System;
using System.Collections.Generic;
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
        private DataStore dataStore;
        private Dictionary<string, Func<T, object>> getters = new Dictionary<string, Func<T, object>>();
        private Dictionary<string, Action<T, object>> setters = new Dictionary<string, Action<T, object>>();

        internal Collection(DataStore dataStore, string name) : base(name)
        {
            this.dataStore = dataStore;
            MethodInfo changeType = typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(object), typeof(Type)});

            foreach (PropertyInfo prop in typeof(T).GetProperties())
            {
                if (prop.GetCustomAttribute<DsIgnoreAttribute>() == null)
                {
                    ParameterExpression obj = Expression.Parameter(typeof(T));
                    Expression getter = Expression.Call(obj, prop.GetMethod);
                    Expression box = Expression.Convert(getter, typeof(object));
                    getters[prop.Name] = Expression.Lambda<Func<T, object>>(box, obj).Compile();

                    ParameterExpression valueObj = Expression.Parameter(typeof(object));
                    Expression propertyType = Expression.Constant(prop.PropertyType);
                    Expression value = Expression.Call(changeType, valueObj, propertyType);
                    value = Expression.Convert(value, prop.PropertyType);
                    Expression setter = Expression.Call(obj, prop.SetMethod, value);
                    setters[prop.Name] = Expression.Lambda<Action<T, object>>(setter, obj, valueObj).Compile();
                }
            }

            foreach (FieldInfo field in typeof(T).GetFields())
            {
                if (field.GetCustomAttribute<DsIgnoreAttribute>() == null)
                {
                    ParameterExpression obj = Expression.Parameter(typeof(T));
                    Expression fieldExpr = Expression.Field(obj, field);
                    Expression box = Expression.Convert(fieldExpr, typeof(object));
                    getters[field.Name] = Expression.Lambda<Func<T, object>>(box, obj).Compile();

                    ParameterExpression valueObj = Expression.Parameter(typeof(object));
                    Expression fieldType = Expression.Constant(field.FieldType);
                    Expression value = Expression.Call(changeType, valueObj, fieldType);
                    value = Expression.Convert(value, field.FieldType);
                    Expression assign = Expression.Assign(fieldExpr, value);
                    setters[field.Name] = Expression.Lambda<Action<T, object>>(assign, obj, valueObj).Compile();
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

            string id = await dataStore.StorageService.Add(this, data);
            this.setters["Id"](document, id);
            return id;
        }

        public async Task<T> Get(string id)
        {
            T result = new T();
            var data = await dataStore.StorageService.Get(this, id);
            setters["Id"](result, id);
            foreach ((string name, object value) in data)
            {
                setters[name](result, value);
            }

            return result;
        }
    }
}