using System;
using System.IO;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using NUnit.Framework;
using Tomatwo.DataStore;

namespace DataStoreTests
{
    public class TestBase
    {
        protected DataStore DataStore => Setup.DataStore;
        protected Collection<Account> Accounts => Setup.Accounts;
        protected Collection<Contact> Contacts => Setup.Contacts;

        protected string Canonicalise(object obj)
        {
            string json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            json = Regex.Replace(json, "^\\s*\"Id\".*$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThis\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "^\\s*\"IgnoreThisToo\": *\"ignore\",?$", "", RegexOptions.Multiline);
            json = Regex.Replace(json, "\n+", "\n");
            json = Regex.Replace(json, @",(\s*})", "$1");
            return json;
        }

        protected void Verify(object result, string desired)
        {
            var resultString = Canonicalise(result);
            var desiredString = File.ReadAllText($"../../../results/{desired}");
            Assert.AreEqual(desiredString.Trim(), resultString.Trim());
        }
    }
}
