namespace Medallion.Threading.KeyPrefix;

public static class DistributedReaderWriterLockProviderExtensions
{
    public static IDistributedReaderWriterLockProvider WithKeyPrefix(this IDistributedReaderWriterLockProvider provider, string keyPrefix)
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

        if (provider is KeyPrefixedDistributedReaderWriterLockProvider prefixed)
        {
            // combine the key in advance to minimize indirection
            keyPrefix = prefixed.KeyPrefix + keyPrefix;
            provider = prefixed.Inner;
        }

        return new KeyPrefixedDistributedReaderWriterLockProvider(provider, keyPrefix);
    }
}
