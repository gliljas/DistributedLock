namespace Medallion.Threading.KeyPrefix;

internal sealed class KeyPrefixedDistributedSemaphoreProvider(IDistributedSemaphoreProvider provider, string keyPrefix) : IDistributedSemaphoreProvider
{
    public IDistributedSemaphoreProvider Inner { get; } = provider;
    public string KeyPrefix { get; } = keyPrefix;
    public IDistributedSemaphore CreateSemaphore(string name, int maxCount) => this.Inner.CreateSemaphore(this.KeyPrefix + name, maxCount);
}
