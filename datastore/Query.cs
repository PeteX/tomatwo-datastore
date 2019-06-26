using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Tomatwo.DataStore
{
    public class Query<T> where T : new()
    {
        public Collection<T> Collection { get; private set; }

        private List<Restriction> restrictions = new List<Restriction>();
        private int limit = 0;

        public Query(Collection<T> collection, Expression<Func<T, bool>> select)
        {
            this.Collection = collection;

            // In this case, the client wants all the records in the collection (x => true):
            if (select.Body is ConstantExpression constant && constant.Value is bool boolVal && boolVal == true)
                return;

            handleNode(select.Body);
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

                    restrictions.Add(new Restriction
                    {
                        FieldName = member.Member.Name,
                        Operator = node.NodeType,
                        Value = Expression.Lambda(node.Right).Compile().DynamicInvoke()
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

        public Query<T> Limit(int limit)
        {
            if (limit < 1)
                throw new InvalidOperationException("Query limits must be at least one.");

            if (this.limit != 0)
                throw new InvalidOperationException("Only one limit may be specified in a query.");

            this.limit = limit;
            return this;
        }

        public async Task<T> GetFirst() => (await Limit(2).GetList()).First();
        public async Task<T> GetFirstOrDefault() => (await Limit(2).GetList()).FirstOrDefault();
        public async Task<T> GetSingle() => (await Limit(2).GetList()).Single();
        public async Task<T> GetSingleOrDefault() => (await Limit(2).GetList()).SingleOrDefault();

        public async Task<List<T>> GetList()
        {
            var results = await Collection.DataStore.StorageService.Query(Collection, restrictions, limit);
            return results.Select(doc => Collection.MakeObject(doc)).ToList();
        }
    }
}