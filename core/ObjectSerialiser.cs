using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Tomatwo.DataStore
{
    internal abstract class ObjectSerialiser
    {
        private static Dictionary<Type, ObjectSerialiser> serialisers = new Dictionary<Type, ObjectSerialiser>();

        internal static ObjectSerialiser GetSerialiser(Type type)
        {
            if (serialisers.TryGetValue(type, out var result))
            {
                if (result == null)
                    throw new InvalidOperationException("Mutually recursive types cannot be stored in the database.");

                return result;
            }

            serialisers[type] = null;
            var desiredType = typeof(ObjectSerialiser<>).MakeGenericType(type);
            var constructor = desiredType.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];
            result = (ObjectSerialiser)constructor.Invoke(new object[0]);
            serialisers[type] = result;
            return result;
        }

        internal static ObjectSerialiser GetSerialiser<T>() where T : new() => GetSerialiser(typeof(T));

        internal abstract IDictionary<string, object> Serialise(object input);
        internal abstract object Deserialise(IDictionary<string, object> input);
        internal abstract void SetMember(object obj, string member, object value);
    }

    internal class ObjectSerialiser<T> : ObjectSerialiser where T : new()
    {
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
            else if (memberType.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                // TODO
                Expression propertyType = Expression.Constant(memberType);
                value = Expression.Call(changeType, value, propertyType);
                return Expression.Convert(value, memberType);
            }
            else if (!memberType.IsValueType && memberType != typeof(string))
            {
                ObjectSerialiser child = ObjectSerialiser.GetSerialiser(memberType);
                Expression childExpr = Expression.Constant(child);
                MethodInfo deserialise =
                    child.GetType().GetMethod("Deserialise", BindingFlags.Instance | BindingFlags.NonPublic);
                value = Expression.Convert(value, typeof(IDictionary<string, object>));
                value = Expression.Call(childExpr, deserialise, value);
                return Expression.Convert(value, memberType);
            }
            else
            {
                Expression propertyType = Expression.Constant(memberType);
                value = Expression.Call(changeType, value, propertyType);
                return Expression.Convert(value, memberType);
            }
        }

        private Expression getConverter(Expression value)
        {
            if (value.Type.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>)))
            {
                return Expression.Convert(value, typeof(object));
            }
            else if (value.Type.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                return Expression.Convert(value, typeof(object));
            }
            else if (!value.Type.IsValueType && value.Type != typeof(string))
            {
                ObjectSerialiser child = ObjectSerialiser.GetSerialiser(value.Type);
                Expression childExpr = Expression.Constant(child);
                MethodInfo serialise =
                    child.GetType().GetMethod("Serialise", BindingFlags.Instance | BindingFlags.NonPublic);
                value = Expression.Convert(value, typeof(object));
                value = Expression.Call(childExpr, serialise, value);
                return Expression.Convert(value, typeof(IDictionary<string, object>));
            }
            else
            {
                return Expression.Convert(value, typeof(object));
            }
        }

        internal ObjectSerialiser()
        {
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
                    Expression box = getConverter(getter);
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

        internal override IDictionary<string, object> Serialise(object input)
        {
            if (input == null)
                return null;

            T document = (T)input;
            var data = new Dictionary<string, object>();

            foreach ((string name, var getter) in getters)
            {
                data[name] = getter(document);
            }

            return data;
        }

        internal override object Deserialise(IDictionary<string, object> input)
        {
            if (input == null)
                return null;

            T result = new T();
            foreach ((string name, object value) in input)
            {
                if (setters.TryGetValue(name, out var setter))
                    setter(result, value);
            }

            return result;
        }

        internal override void SetMember(object obj, string member, object value) => setters[member]((T)obj, value);
    }
}
