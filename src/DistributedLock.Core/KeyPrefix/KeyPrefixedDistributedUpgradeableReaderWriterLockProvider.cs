namespace Medallion.Threading.KeyPrefix;

internal sealed class KeyPrefixedDistributedUpgradeableReaderWriterLockProvider(IDistributedUpgradeableReaderWriterLockProvider provider, string keyPrefix) : IDistributedUpgradeableReaderWriterLockProvider
{
    public IDistributedUpgradeableReaderWriterLockProvider Inner { get; } = provider;
    public string KeyPrefix { get; } = keyPrefix;
    public IDistributedUpgradeableReaderWriterLock CreateUpgradeableReaderWriterLock(string name) => this.Inner.CreateUpgradeableReaderWriterLock(this.KeyPrefix + name);
    public IDistributedReaderWriterLock CreateReaderWriterLock(string name) => this.Inner.CreateReaderWriterLock(this.KeyPrefix + name);
}
