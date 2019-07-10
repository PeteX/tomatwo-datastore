using System.Reflection;
using System.Threading.Tasks;
using Tomatwo.DependencyInjection;

namespace Tomatwo.DataStore
{
    public class TransactionInterceptor
    {
        private DataStore dataStore;

        public TransactionInterceptor(DataStore dataStore)
        {
            this.dataStore = dataStore;
        }

        public object TypedInterceptor<T>(Interception details)
        {
            T result = default(T);

            Task task = dataStore.RunTransaction(async () =>
            {
                result = await (Task<T>)details.Invoke(details.Target, details.Args);
            });

            return task.ContinueWith(_ => result);
        }

        public object Interceptor(Interception details)
        {
            MethodInfo interceptorT = GetType().GetMethod("TypedInterceptor");
            MethodInfo interceptor = interceptorT.MakeGenericMethod(details.Method.ReturnType.GenericTypeArguments[0]);
            return interceptor.Invoke(this, new object[] { details });
        }
    }
}
