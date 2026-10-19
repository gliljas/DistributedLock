namespace Medallion.Threading.KeyPrefix;

internal sealed class KeyPrefixedDistributedLockProvider(IDistributedLockProvider provider, string keyPrefix) : IDistributedLockProvider
{
    public IDistributedLockProvider Inner { get; } = provider;
    public string KeyPrefix { get; } = keyPrefix;
    public IDistributedLock CreateLock(string name) => this.Inner.CreateLock(this.KeyPrefix + name);
}
