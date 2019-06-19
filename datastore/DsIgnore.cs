using System;

namespace Tomatwo.DataStore
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class DsIgnoreAttribute : Attribute
    {
    }
}
