using System.Reflection;

namespace Tomatwo.DataStore
{
    public struct SortKey
    {
        public MemberInfo Field;
        public bool Ascending;
    }
}
