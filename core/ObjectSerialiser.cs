using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Tomatwo.DataStore
{
    // The ObjectSerialiser converts between POCOs and nested IDictionaries and ILists.  It emits code to do the
    // conversion at setup time, rather than using reflection at the time when the database is accessed.  This increases
    // efficiency at the cost of higher complexity.

    internal abstract class ObjectSerialiser
    {
        private static Dictionary<Type, ObjectSerialiser> serialisers = new Dictionary<Type, ObjectSerialiser>();

        // Get a new ObjectSerialiser.  Creating an ObjectSerialiser for a type will also cause ObjectSerialisers to be
        // created for any child types.  To protect against loops, a distinction is drawn between there being no cached
        // serialiser (in which case it must be created) and the cached serialiser being null.  If it is null, the
        // serialiser is in the process of being created, so attempting to retrieve it indicates a loop.
        //
        // It is helpful to have the serialiser parameterised on the type being serialised.  Unfortunately this means it
        // must be created using reflection, because sometimes the desired parameter type is not known at compile time.

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
        // Serialisation and deserialisation are similar in some respects.  To avoid confusion, the code uses "get" for
        // things that relate to serialisation, and "set" for things that relate to deserialisation.  This is because
        // serialisation involves T's getters, and deserialisation the setters.

        private Dictionary<string, Func<T, object>> getters = new Dictionary<string, Func<T, object>>();
        private Dictionary<string, Action<T, object>> setters = new Dictionary<string, Action<T, object>>();

        // These static methods are used to serialise and deserialise various collection types.  This is involved enough
        // that it is difficult to express directly using System.Linq.Expressions.  Because they handle collections,
        // they are parameterised on the type contained in the collection.  (Some are also parameterised on the
        // collection type itself).  The specialisation is done using reflection, because the types are not known at
        // compile time.

        private static IList<object> listTypeGetter<TInner>(object input, Func<object, object> innerSerialiser)
        {
            if (input == null)
                return null;

            IList<object> result = new List<object>();

            foreach (object obj in (IList<TInner>)input)
            {
                result.Add(innerSerialiser(obj));
            }

            return result;
        }

        private static TOuter listTypeSetter<TOuter, TInner>(object input, Func<object, object> innerDeserialiser)
            where TOuter : IList<TInner>, new()
        {
            if (input == null)
                return default(TOuter);

            TOuter result = new TOuter();

            foreach (object obj in (IList<object>)input)
            {
                result.Add((TInner)innerDeserialiser(obj));
            }

            return result;
        }

        private static IList<object> arrayTypeGetter<TInner>(object input, Func<object, object> innerSerialiser)
        {
            if (input == null)
                return null;

            IList<object> result = new List<object>();

            foreach (object obj in (TInner[])input)
            {
                result.Add(innerSerialiser(obj));
            }

            return result;
        }

        private static TInner[] arrayTypeSetter<TInner>(object input, Func<object, object> innerDeserialiser)
        {
            if (input == null)
                return null;

            IList<object> list = (IList<object>)input;
            TInner[] result = new TInner[list.Count];

            for (int i = 0; i < list.Count; i++)
            {
                result[i] = (TInner)innerDeserialiser(list[i]);
            }

            return result;
        }

        private static IDictionary<string, object> dictTypeGetter<TInner>(
            object input, Func<object, object> innerSerialiser)
        {
            if (input == null)
                return null;

            IDictionary<string, object> result = new Dictionary<string, object>();

            foreach ((string key, object value) in (IDictionary<string, TInner>)input)
            {
                result[key] = innerSerialiser(value);
            }

            return result;
        }

        private static TOuter dictTypeSetter<TOuter, TInner>(object input, Func<object, object> innerDeserialiser)
            where TOuter : IDictionary<string, TInner>, new()
        {
            if (input == null)
                return default(TOuter);

            TOuter result = new TOuter();

            foreach ((string key, object value) in (IDictionary<string, object>)input)
            {
                result[key] = (TInner)innerDeserialiser(value);
            }

            return result;
        }

        // The getConverter is an Expression that serialises a particular member.  For atomic types it is trivial but
        // arrays, lists, dictionaries and child objects are all more complex.  The function is directly recursive when
        // handling collections, because it needs to serialise not just the collection but the members too.  The
        // function is indirectly recursive when handling child objects; it allocates a new ObjectSerialiser and this
        // will call getConverter in due course.
        //
        // Essentially the collections work by specialising the appropriate xxxTypeGetter method, then calling it with a
        // getConverter that is capable of handling the contained objects.

        private Expression getConverter(Expression value)
        {
            if (value.Type.IsArray)
            {
                Type arrayContent = value.Type.GetElementType();
                MethodInfo method =
                    GetType().GetMethod("arrayTypeGetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(arrayContent);

                var arg = Expression.Parameter(typeof(object));
                var typedArg = Expression.Convert(arg, arrayContent);
                var innerSerialiser = (Func<object, object>)Expression.Lambda(getConverter(typedArg), arg).Compile();
                var innerSerialiserExpr = Expression.Constant(innerSerialiser);
                return Expression.Call(null, specialised, value, innerSerialiserExpr);
            }
            else if (value.Type.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IList<>)))
            {
                Type listContent = value.Type.GetGenericArguments()[0];
                MethodInfo method = GetType().GetMethod("listTypeGetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(listContent);

                var arg = Expression.Parameter(typeof(object));
                var typedArg = Expression.Convert(arg, listContent);
                var innerSerialiser = (Func<object, object>)Expression.Lambda(getConverter(typedArg), arg).Compile();
                var innerSerialiserExpr = Expression.Constant(innerSerialiser);
                return Expression.Call(null, specialised, value, innerSerialiserExpr);
            }
            else if (value.Type.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                Type dictValue = value.Type.GetGenericArguments()[1];
                MethodInfo method = GetType().GetMethod("dictTypeGetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(dictValue);

                var arg = Expression.Parameter(typeof(object));
                var typedArg = Expression.Convert(arg, dictValue);
                var innerSerialiser = (Func<object, object>)Expression.Lambda(getConverter(typedArg), arg).Compile();
                var innerSerialiserExpr = Expression.Constant(innerSerialiser);
                return Expression.Call(null, specialised, value, innerSerialiserExpr);
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

        // The setConverter is the opposite of getConverter, deserialising one member in much the same way.

        private Expression setConverter(Expression value, Type memberType)
        {
            MethodInfo changeType =
                typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(object), typeof(Type) });

            if (memberType.IsArray)
            {
                Type arrayContent = memberType.GetElementType();
                MethodInfo method =
                    GetType().GetMethod("arrayTypeSetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(arrayContent);

                var arg = Expression.Parameter(typeof(object));
                var converter = setConverter(arg, arrayContent);
                converter = Expression.Convert(converter, typeof(object));
                var innerDeserialiser = (Func<object, object>)Expression.Lambda(converter, arg).Compile();
                var innerDeserialiserExpr = Expression.Constant(innerDeserialiser);
                return Expression.Call(null, specialised, value, innerDeserialiserExpr);
            }
            else if (memberType.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IList<>)))
            {
                Type listContent = memberType.GetGenericArguments()[0];
                MethodInfo method = GetType().GetMethod("listTypeSetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(memberType, listContent);

                var arg = Expression.Parameter(typeof(object));
                var converter = setConverter(arg, listContent);
                converter = Expression.Convert(converter, typeof(object));
                var innerDeserialiser = (Func<object, object>)Expression.Lambda(converter, arg).Compile();
                var innerDeserialiserExpr = Expression.Constant(innerDeserialiser);
                return Expression.Call(null, specialised, value, innerDeserialiserExpr);
            }
            else if (memberType.GetInterfaces().Any(x => x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                // The keys must always be strings, as this is required by JSON.
                Type dictValue = memberType.GetGenericArguments()[1];
                MethodInfo method = GetType().GetMethod("dictTypeSetter", BindingFlags.NonPublic | BindingFlags.Static);
                MethodInfo specialised = method.MakeGenericMethod(memberType, dictValue);

                var arg = Expression.Parameter(typeof(object));
                var converter = setConverter(arg, dictValue);
                converter = Expression.Convert(converter, typeof(object));
                var innerDeserialiser = (Func<object, object>)Expression.Lambda(converter, arg).Compile();
                var innerDeserialiserExpr = Expression.Constant(innerDeserialiser);
                return Expression.Call(null, specialised, value, innerDeserialiserExpr);
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

        // When an ObjectSerialiser is created, the constructor iterates through the public properties and fields.  It
        // creates lambda expressions to serialise and deserialise each of them.

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

        // Serialise and Deserialise use the functions created above to serialise and deserialise a whole object.

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

        // SetMember is used for setting the Id field, for example after adding a new record.

        internal override void SetMember(object obj, string member, object value) => setters[member]((T)obj, value);
    }
}
