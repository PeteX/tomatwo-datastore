using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Tomatwo.DataStore
{
    public class Query<T> where T : new()
    {
        public Collection<T> Collection { get; private set; }

        private class UnknownNodeException : Exception
        {
        }

        internal bool FastPath = false;
        internal bool Cacheable = false;
        internal bool Cached = false;
        internal StringBuilder Shape;

        private static ConcurrentDictionary<string, Delegate> cache = new ConcurrentDictionary<string, Delegate>();

        private List<ParameterExpression> paramEx;
        private List<object> param;

        private List<Restriction> restrictions = new List<Restriction>();
        private List<SortKey> sortKeys = new List<SortKey>();
        private int limit = 0;
        private List<object> startAfter = new List<object>();

        public Query(Collection<T> collection, Expression<Func<T, bool>> select)
        {
            this.Collection = collection;

            // In this case, the client wants all the records in the collection (x => true):
            if (select.Body is ConstantExpression constant && constant.Value is bool boolVal && boolVal == true)
                return;

            handleNode(select.Body);
        }


        private StringBuilder escapeStr(string input)
        {
            StringBuilder result = new StringBuilder();

            foreach (char ch in input)
            {
                switch (ch)
                {
                    case '\\': result.Append(@"\\"); break;
                    case '}': result.Append(@"\}"); break;
                    default: result.Append(ch); break;
                }
            }

            return result;
        }

        private Expression valueNode(Expression exp)
        {
            Expression result;

            Shape.Append('{');
            switch (exp)
            {
                case ConstantExpression constant:
                    Shape.Append(constant.Type.Name);
                    Shape.Append(',');
                    Shape.Append(escapeStr(constant.Value.ToString()));
                    result = constant;
                    break;

                case MemberExpression member:
                    Shape.Append(member.Type);

                    var resultParam = Expression.Parameter(member.Type);
                    result = resultParam;
                    paramEx.Add(resultParam);

                    object value = null;
                    if (member.Expression is ConstantExpression c)
                        value = c.Value;

                    if (value == null)
                        throw new UnknownNodeException();

                    param.Add(member.Member switch
                    {
                        FieldInfo field => field.GetValue(value),
                        PropertyInfo property => property.GetValue(value),
                        _ => throw new UnknownNodeException()
                    });

                    break;

                case BinaryExpression binary:
                    Expression left = valueNode(binary.Left);
                    Expression right = valueNode(binary.Right);
                    switch (binary.NodeType)
                    {
                        case ExpressionType.Add:
                            Shape.Append('+');
                            result = Expression.MakeBinary(
                                binary.NodeType, left, right, binary.IsLiftedToNull, binary.Method);
                            break;

                        case ExpressionType.Subtract:
                            Shape.Append('-');
                            result = Expression.MakeBinary(
                                binary.NodeType, left, right, binary.IsLiftedToNull, binary.Method);
                            break;

                        case ExpressionType.Multiply:
                            Shape.Append('*');
                            result = Expression.MakeBinary(
                                binary.NodeType, left, right, binary.IsLiftedToNull, binary.Method);
                            break;

                        case ExpressionType.Divide:
                            Shape.Append('/');
                            result = Expression.MakeBinary(
                                binary.NodeType, left, right, binary.IsLiftedToNull, binary.Method);
                            break;

                        default:
                            throw new UnknownNodeException();
                    };

                    break;

                default:
                    throw new UnknownNodeException();
            }

            Shape.Append('}');
            return result;
        }

        private void handleNode(Expression exp)
        {
            if (exp is BinaryExpression node)
            {
                if (node.NodeType == ExpressionType.AndAlso)
                {
                    handleNode(node.Left);
                    handleNode(node.Right);
                }
                else if (
                    node.NodeType == ExpressionType.LessThan ||
                    node.NodeType == ExpressionType.LessThanOrEqual ||
                    node.NodeType == ExpressionType.Equal ||
                    node.NodeType == ExpressionType.GreaterThanOrEqual ||
                    node.NodeType == ExpressionType.GreaterThan)
                {
                    if (!(node.Left is MemberExpression member))
                    {
                        throw new InvalidOperationException(
                            "Query expression contained a comparison which was not with one of the record fields.");
                    }

                    // Having established the record field we are comparing, we need to work out what we are comparing
                    // with.  Unfortunately this is a bit slow if done in the obvious way, so we have three different
                    // options.
                    //
                    // 1.  If the value is a constant or a member access (which includes local variables in this
                    //     context) just get the value.

                    object value = null;
                    switch (node.Right)
                    {
                        case ConstantExpression constant:
                            value = constant.Value;
                            break;

                        case MemberExpression member1:
                            if (member1.Expression is ConstantExpression constant1)
                            {
                                value = member1.Member switch
                                {
                                    FieldInfo f => f.GetValue(constant1.Value),
                                    PropertyInfo p => p.GetValue(constant1.Value),
                                    _ => null
                                };
                            }

                            break;
                    };

                    try
                    {
                        if (value != null)
                        {
                            FastPath = true;
                        }
                        else
                        {
                            // 2.  Compile the expression but create a lambda which can be cached.  To do this, we
                            //     descend the tree and create two things.  We create a string which describes the
                            //     "shape" of the expression, that is to say, the nodes and their hierarchy.  We also
                            //     create a new expression tree, with parameters in place of member access nodes.  This
                            //     can be turned into a lambda expression later.

                            Shape = new StringBuilder();
                            paramEx = new List<ParameterExpression>();
                            param = new List<object>();

                            var valueExp = valueNode(node.Right);
                            Delegate dlg;
                            string shapeStr = Shape.ToString();
                            if (cache.TryGetValue(shapeStr, out dlg))
                            {
                                Cached = true;
                            }
                            else
                            {
                                var lambda = Expression.Lambda(valueExp, paramEx);
                                dlg = lambda.Compile();
                                cache[shapeStr] = dlg;
                            }

                            Cacheable = true;
                            value = dlg.DynamicInvoke(param.ToArray());
                        }
                    }
                    catch (UnknownNodeException)
                    {
                        // 3.  The above code doesn't know about all the nuances of expression trees, so it may fail and
                        //     throw UnknownNodeException.  In that case there is no option but to do it the obvious
                        //     way, just compiling the expression and running it.  This is the slowest option, often
                        //     hundreds of microseconds.  Unfortunately it is not possible to cache these, because there
                        //     is no usable key for the cache.

                        var dlg = Expression.Lambda(node.Right).Compile();
                        value = dlg.DynamicInvoke();
                    }

                    restrictions.Add(new Restriction
                    {
                        FieldName = member.Member.Name,
                        Operator = node.NodeType,
                        Value = value
                    });
                }
                else
                {
                    throw new InvalidOperationException("Query expression contained an unsupported operator.");
                }
            }
            else
            {
                throw new InvalidOperationException("Query expression contained an unsupported operation.");
            }
        }

        private string selectField(Expression<Func<T, object>> exp)
        {
            const string error = "Field selection lambda must be of the form x => x.MemberName.";

            MemberExpression member = exp.Body switch
            {
                UnaryExpression convert => convert.Operand as MemberExpression,
                MemberExpression m => m,
                _ => throw new InvalidOperationException(error)
            };

            if (member == null)
                throw new InvalidOperationException(error);

            return member.Member.Name;
        }

        public Query<T> Limit(int limit)
        {
            if (limit < 1)
                throw new InvalidOperationException("Query limits must be at least one.");

            if (this.limit != 0)
                throw new InvalidOperationException("Only one limit may be specified in a query.");

            this.limit = limit;
            return this;
        }

        public Query<T> StartAfter(object start)
        {
            startAfter.Add(start);
            return this;
        }

        public Query<T> OrderBy(Expression<Func<T, object>> exp)
        {
            sortKeys.Add(new SortKey { FieldName = selectField(exp), Ascending = true });
            return this;
        }

        public Query<T> OrderByDescending(Expression<Func<T, object>> exp)
        {
            sortKeys.Add(new SortKey { FieldName = selectField(exp), Ascending = false });
            return this;
        }

        public async Task<T> GetFirst() => (await Limit(2).GetList()).First();
        public async Task<T> GetFirstOrDefault() => (await Limit(2).GetList()).FirstOrDefault();
        public async Task<T> GetSingle() => (await Limit(2).GetList()).Single();
        public async Task<T> GetSingleOrDefault() => (await Limit(2).GetList()).SingleOrDefault();

        public async Task<List<T>> GetList()
        {
            var results = await Collection.DataStore.StorageService.Query(Collection, restrictions, sortKeys, limit,
                startAfter);

            return results.Select(doc => Collection.Deserialise(doc)).ToList();
        }
    }
}