using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Medallion.Threading.Internal;

namespace Medallion.Threading.Azure;

/// <summary>
/// Adds <see cref="SyncViaAsync"/> support to <see cref="BlobBaseClient"/>
/// </summary>
internal class BlobClientWrapper
{
    private readonly BlobBaseClient _blobClient;
    private static readonly BlobRequestConditions IfNoneMatchAll = new BlobRequestConditions { IfNoneMatch = ETag.All };


    public BlobClientWrapper(BlobBaseClient blobClient)
    {
        this._blobClient = blobClient;
    }

    public string Name => this._blobClient.Name;

    public BlobLeaseClientWrapper GetBlobLeaseClient() => new(this._blobClient.GetBlobLeaseClient());

    public async ValueTask<IDictionary<string, string>> GetMetadataAsync(string leaseId, CancellationToken cancellationToken)
    {
        var conditions = new BlobRequestConditions { LeaseId = leaseId };
        var properties = SyncViaAsync.IsSynchronous
            ? this._blobClient.GetProperties(conditions, cancellationToken)
            : await this._blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
        return properties.Value.Metadata;
    }

    public async ValueTask<bool> CreateIfNotExistsAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        switch (this._blobClient)
        {
            case BlobClient blobClient:
                try
                {
                    if (SyncViaAsync.IsSynchronous)
                    {
                        blobClient.Upload(Stream.Null, metadata: metadata, conditions: IfNoneMatchAll, cancellationToken: cancellationToken);
                    }
                    else
                    {
                        await blobClient.UploadAsync(Stream.Null, metadata: metadata, conditions: IfNoneMatchAll, cancellationToken: cancellationToken);
                    }
                    return true;
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
                {
                    return false;
                }
            case BlockBlobClient blockBlobClient:
                try
                {
                    if (SyncViaAsync.IsSynchronous)
                    {
                        blockBlobClient.Upload(Stream.Null, metadata: metadata, conditions: IfNoneMatchAll, cancellationToken: cancellationToken);
                    }
                    else
                    {
                        await blockBlobClient.UploadAsync(Stream.Null, metadata: metadata, conditions: IfNoneMatchAll, cancellationToken: cancellationToken).ConfigureAwait(false);
                    }
                    return true;
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
                {
                    return false;
                }
            case PageBlobClient pageBlobClient:
                if (SyncViaAsync.IsSynchronous)
                {
                    return pageBlobClient.CreateIfNotExists(size: 0, metadata: metadata, cancellationToken: cancellationToken) != null;
                }
                return (await pageBlobClient.CreateIfNotExistsAsync(size: 0, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false)) != null;
            case AppendBlobClient appendBlobClient:
                if (SyncViaAsync.IsSynchronous)
                {
                    return appendBlobClient.CreateIfNotExists(metadata: metadata, cancellationToken: cancellationToken) != null;
                }
                return (await appendBlobClient.CreateIfNotExistsAsync(metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false)) != null;
            default:
                throw new InvalidOperationException(
                    this._blobClient.GetType() == typeof(BlobBaseClient)
                        ? $"Unable to create a lock blob given client type {typeof(BlobBaseClient)}. Either ensure that the blob exists or use a non-base client type such as {typeof(BlobClient)}"
                            + " which specifies the type of blob to create"
                        : $"Unexpected blob client type {this._blobClient.GetType()}"
                );
        }
    }

    public ValueTask DeleteIfExistsAsync(string? leaseId = null)
    {
        var conditions = leaseId != null ? new BlobRequestConditions { LeaseId = leaseId } : null;
        if (SyncViaAsync.IsSynchronous)
        {
            this._blobClient.DeleteIfExists(conditions: conditions);
            return default;
        }
        return new ValueTask(this._blobClient.DeleteIfExistsAsync(conditions: conditions));
    }
}
