namespace Medallion.Threading.KeyPrefix;

public static class DistributedSemaphoreProviderExtensions
{
    public static IDistributedSemaphoreProvider WithKeyPrefix(this IDistributedSemaphoreProvider provider, string keyPrefix)
    {
        if (provider == null)
        {
            throw new ArgumentNullException(nameof(provider));
        }

        if (keyPrefix == null)
        {
            throw new ArgumentNullException(nameof(keyPrefix));
        }

        if (keyPrefix == string.Empty)
        {
            return provider;
        }

        if (provider is KeyPrefixedDistributedSemaphoreProvider prefixed)
        {
            // combine the key in advance to minimize indirection
            keyPrefix = prefixed.KeyPrefix + keyPrefix;
            provider = prefixed.Inner;
        }

        return new KeyPrefixedDistributedSemaphoreProvider(provider, keyPrefix);
    }
}
