namespace Medallion.Threading.KeyPrefix;

internal sealed class KeyPrefixedDistributedReaderWriterLockProvider(IDistributedReaderWriterLockProvider provider, string keyPrefix) : IDistributedReaderWriterLockProvider
{
    public IDistributedReaderWriterLockProvider Inner { get; } = provider;
    public string KeyPrefix { get; } = keyPrefix;
    public IDistributedReaderWriterLock CreateReaderWriterLock(string name) => this.Inner.CreateReaderWriterLock(this.KeyPrefix + name);
}
