namespace Tomatwo.DataStore.StorageServices.Firestore
{
    public class FirestoreStorageOptions
    {
        public string CredentialFile { get; set; }
        public string Prefix { get; set; }
        public string Project { get; internal set; }
    }
}
