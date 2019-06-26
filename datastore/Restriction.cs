using System.Linq.Expressions;

namespace Tomatwo.DataStore
{
    public struct Restriction
    {
        public string FieldName;
        public ExpressionType Operator;
        public object Value;
    }
}
