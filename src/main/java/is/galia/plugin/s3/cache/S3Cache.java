/*
 * Copyright © 2024 Baird Creek Software LLC
 *
 * Licensed under the PolyForm Noncommercial License, version 1.0.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://polyformproject.org/licenses/noncommercial/1.0.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package is.galia.plugin.s3.cache;

import is.galia.async.VirtualThreadPool;
import is.galia.cache.AbstractCache;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.cache.VariantCache;
import is.galia.config.Configuration;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.MediaType;
import is.galia.image.StatResult;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.Plugin;
import is.galia.plugin.s3.util.IOUtils;
import is.galia.plugin.s3.util.S3ClientBuilder;
import is.galia.plugin.s3.util.S3Utils;
import is.galia.plugin.s3.config.Key;
import is.galia.util.Stopwatch;
import is.galia.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * <p>LRU cache using an S3 bucket.</p>
 *
 * <p>Object keys are named according to the following template:</p>
 *
 * <dl>
 *     <dt>Images</dt>
 *     <dd><code>{@link Key#S3CACHE_OBJECT_KEY_PREFIX}/image/{op list string
 *     representation}</code></dd>
 *     <dt>Info</dt>
 *     <dd><code>{@link Key#S3CACHE_OBJECT_KEY_PREFIX}/info/{identifier}.json</code></dd>
 * </dl>
 *
 * <p>S3 does not support a last-accessed time as part of its native object
 * metadata, and S3 objects are immutable&mdash;but tags aren't, so they are
 * used to record this information.</p>
 *
 * <p>Image uploads use S3 multipart uploads; see {@link
 * S3MultipartAsyncOutputStream}.</p>
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/">
 *     AWS SDK for Java API Reference</a>
 */
public final class S3Cache extends AbstractCache
        implements VariantCache, InfoCache, Plugin {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(S3Cache.class);

    static final String IMAGE_KEY_PREFIX          = "image/";
    static final String INFO_KEY_PREFIX           = "info/";
    static final String INFO_EXTENSION            = ".json";
    static final String LAST_ACCESS_TIME_TAG_NAME = "LastAccessTime";
    private static final int DEFAULT_MAX_RETRIES  = 5;

    /**
     * Lazy-initialized by {@link #getClientInstance}.
     */
    private static S3Client client;

    public static synchronized S3Client getClientInstance() {
        if (client == null) {
            final Configuration config = Configuration.forApplication();
            client = new S3ClientBuilder()
                    .accessKeyID(config.getString(Key.S3CACHE_ACCESS_KEY_ID.key()))
                    .secretAccessKey(config.getString(Key.S3CACHE_SECRET_ACCESS_KEY.key()))
                    .endpoint(config.getString(Key.S3CACHE_ENDPOINT.key()))
                    .region(config.getString(Key.S3CACHE_REGION.key()))
                    .asyncCredentialUpdateEnabled(isAsyncCredentialUpdateEnabled())
                    .build();
        }
        return client;
    }

    /**
     * @return Earliest valid instant, with second resolution.
     */
    private static Instant earliestValidInstant() {
        final Configuration config = Configuration.forApplication();
        final long ttl = config.getLong(is.galia.config.Key.VARIANT_CACHE_TTL);
        return (ttl > 0) ? Instant.now().minusSeconds(ttl) : Instant.EPOCH;
    }

    /**
     * Bursted requests to AWS are known to occasionally fail with a 503
     * response ("Please reduce your request rate").
     */
    private static int getMaxRetries() {
        Configuration config = Configuration.forApplication();
        return config.getInt(Key.S3CACHE_MAX_RETRIES.key(),
                DEFAULT_MAX_RETRIES);
    }

    private static boolean isAsyncCredentialUpdateEnabled() {
        Configuration config = Configuration.forApplication();
        return config.getBoolean(Key.S3CACHE_ASYNC_CREDENTIAL_UPDATE.key(), true);
    }

    private static boolean isUsingMultipartUploads() {
        Configuration config = Configuration.forApplication();
        return config.getBoolean(Key.S3CACHE_MULTIPART_UPLOADS.key(), false);
    }

    private static boolean isValid(Instant lastModified) {
        Instant earliestAllowed = earliestValidInstant();
        return lastModified.isAfter(earliestAllowed);
    }

    static Tag newLastAccessedTag() {
        return Tag.builder()
                .key(LAST_ACCESS_TIME_TAG_NAME)
                .value(String.valueOf(Instant.now().toEpochMilli()))
                .build();
    }

    static String getBucket() {
        return Configuration.forApplication().getString(Key.S3CACHE_BUCKET.key());
    }

    /**
     * @return Object key of the serialized {@link Info} associated with the
     *         given identifier.
     */
    String getObjectKey(Identifier identifier) {
        return getObjectKeyPrefix() + INFO_KEY_PREFIX +
                StringUtils.md5(identifier.toString()) + INFO_EXTENSION;
    }

    /**
     * @return Object key of the variant image associated with the given
     *         operation list.
     */
    String getObjectKey(OperationList opList) {
        final String idHash  = StringUtils.md5(opList.getIdentifier().toString());
        final String opsHash = StringUtils.md5(opList.toString());

        String extension = "";
        Encode encode = (Encode) opList.getFirst(Encode.class);
        if (encode != null) {
            extension = "." + encode.getFormat().getPreferredExtension();
        }
        return getObjectKeyPrefix() + IMAGE_KEY_PREFIX + idHash + "/" +
                opsHash + extension;
    }

    /**
     * @return Value of {@link Key#S3CACHE_OBJECT_KEY_PREFIX} with trailing
     *         slash.
     */
    String getObjectKeyPrefix() {
        String prefix = Configuration.forApplication().
                getString(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "");
        if (prefix.isEmpty() || prefix.equals("/")) {
            return "";
        }
        return StringUtils.stripEnd(prefix, "/") + "/";
    }

    private boolean isValid(S3Object object) {
        final S3Client client   = getClientInstance();
        final String bucketName = getBucket();
        GetObjectTaggingRequest getTaggingRequest = GetObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(object.key())
                .build();
        GetObjectTaggingResponse taggingResponse =
                client.getObjectTagging(getTaggingRequest);
        Tag tag = taggingResponse.tagSet()
                .stream()
                .filter(t -> t.key().equals(LAST_ACCESS_TIME_TAG_NAME))
                .findFirst()
                .orElse(null);
        if (tag != null) {
            return isValid(Instant.ofEpochMilli(Long.parseLong(tag.value())));
        }
        return false;
    }

    private void evict(final String objectKey) {
        final S3Client client = getClientInstance();
        client.deleteObject(DeleteObjectRequest.builder()
                .bucket(getBucket())
                .key(objectKey)
                .build());
    }

    private void evictAsync(final String bucketName, final String key) {
        VirtualThreadPool.getInstance().submit(() -> {
            final S3Client client = getClientInstance();
            LOGGER.debug("evictAsync(): deleting {} from bucket {}",
                    key, bucketName);
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            return null;
        });
    }

    /**
     * Updates an object's last-accessed time. Since S3 doesn't support a
     * native last-accessed time and S3 objects are immutable, a tag is used.
     */
    private void touchAsync(String objectKey) {
        final S3Client client   = getClientInstance();
        final String bucketName = getBucket();
        VirtualThreadPool.getInstance().submit(() -> {
            LOGGER.trace("touchAsync(): {}", objectKey);

            Tagging tagging = Tagging.builder()
                    .tagSet(newLastAccessedTag())
                    .build();
            PutObjectTaggingRequest taggingRequest = PutObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .tagging(tagging)
                    .build();
            client.putObjectTagging(taggingRequest);
        });
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(S3Cache.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return getClass().getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void initializePlugin() {}

    @Override
    public void onApplicationStop() {}

    //endregion
    //region Cache methods

    @Override
    public void evict(final Identifier identifier) {
        // Evict the info
        evict(getObjectKey(identifier));

        // Evict images
        final S3Client client       = getClientInstance();
        final String bucketName     = getBucket();
        final String prefix         = getObjectKeyPrefix() + IMAGE_KEY_PREFIX +
                StringUtils.md5(identifier.toString());
        final AtomicInteger counter = new AtomicInteger();

        S3Utils.walkObjects(client, bucketName, prefix, (object) -> {
            LOGGER.trace("evict(Identifier): deleting {}", object.key());
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(object.key())
                    .build());
            counter.incrementAndGet();
        });
        LOGGER.debug("evict(Identifier): deleted {} items", counter.get());
    }

    @Override
    public void evictInvalid() {
        final S3Client client              = getClientInstance();
        final String bucketName            = getBucket();
        final AtomicInteger counter        = new AtomicInteger();
        final AtomicInteger deletedCounter = new AtomicInteger();

        S3Utils.walkObjects(client, bucketName, getObjectKeyPrefix(), (object) -> {
            counter.incrementAndGet();
            if (!isValid(object)) {
                try {
                    client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(object.key())
                            .build());
                    deletedCounter.incrementAndGet();
                } catch (S3Exception e) {
                    LOGGER.warn("evictInvalid(): {}", e.getMessage());
                }
            }
        });
        LOGGER.debug("evictInvalid(): deleted {} of {} items",
                deletedCounter.get(), counter.get());
    }

    @Override
    public void purge() {
        final S3Client client       = getClientInstance();
        final String bucketName     = getBucket();
        final AtomicInteger counter = new AtomicInteger();

        S3Utils.walkObjects(client, bucketName, getObjectKeyPrefix(), (object) -> {
            try {
                client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(object.key())
                        .build());
                counter.incrementAndGet();
            } catch (S3Exception e) {
                LOGGER.warn("purge(): {}", e.getMessage());
            }
        });
        LOGGER.debug("purge(): deleted {} items", counter.get());
    }

    @Override
    public void shutdown() {
        getClientInstance().close();
    }

    //endregion
    //region InfoCache methods

    @Override
    public void evictInfos() {
        final S3Client client       = getClientInstance();
        final String bucketName     = getBucket();
        final String prefix         = getObjectKeyPrefix() + INFO_KEY_PREFIX;
        final AtomicInteger counter = new AtomicInteger();

        S3Utils.walkObjects(client, bucketName, prefix, (object) -> {
            LOGGER.trace("evictInfos(): deleting {}", object.key());
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(object.key())
                    .build());
            counter.incrementAndGet();
        });
        LOGGER.debug("evictInfos(): deleted {} items", counter.get());
    }

    @Override
    public Optional<Info> fetchInfo(Identifier identifier) throws IOException {
        final S3Client client          = getClientInstance();
        final String bucketName        = getBucket();
        final String objectKey         = getObjectKey(identifier);
        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .ifModifiedSince(earliestValidInstant())
                .build();
        final Stopwatch watch = new Stopwatch();
        try (ResponseInputStream<GetObjectResponse> is = client.getObject(request)) {
            final Info info = Info.fromJSON(is);
            // Populate the serialization timestamp if it is not already,
            // as suggested by the method contract.
            if (info.getSerializationTimestamp() == null) {
                info.setSerializationTimestamp(is.response().lastModified());
            }
            LOGGER.debug("fetchInfo(): read {} from bucket {} in {}",
                    objectKey, bucketName, watch);
            touchAsync(objectKey);
            return Optional.of(info);
        } catch (S3Exception e) {
            if (e.statusCode() != 304 && e.statusCode() != 404) {
                throw new IOException(e);
            }
        } catch (SdkException e) {
            throw new IOException(e);
        }
        return Optional.empty();
    }

    /**
     * Uploads the given info to S3.
     *
     * @param identifier Image identifier.
     * @param info       Info to upload to S3.
     */
    @Override
    public void put(Identifier identifier, Info info) throws IOException {
        put(identifier, info.toJSON());
    }

    /**
     * Uploads the given info to S3.
     *
     * @param identifier Image identifier.
     * @param info       Info to upload to S3.
     */
    @Override
    public void put(Identifier identifier, String info) throws IOException {
        retryPut(identifier, info, 0);
    }

    private void retryPut(Identifier identifier,
                          String info,
                          int retryCount) throws IOException {
        LOGGER.debug("put(): caching info for {}", identifier);
        final Stopwatch watch = new Stopwatch();

        final String bucket      = getBucket();
        final String key         = getObjectKey(identifier);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(MediaType.APPLICATION_JSON.toString())
                .contentEncoding("UTF-8")
                .build();
        byte[] data = info.getBytes(StandardCharsets.UTF_8);

        LOGGER.debug("put(): uploading {} bytes to {} in bucket {}",
                data.length, request.key(), request.bucket());

        try (ByteArrayInputStream is = new ByteArrayInputStream(data)) {
            getClientInstance().putObject(request,
                    RequestBody.fromInputStream(is, data.length));
            touchAsync(key);
        } catch (S3Exception e) {
            if (retryCount <= getMaxRetries() && e.statusCode() == 503 &&
                    e.getMessage().contains("reduce your request rate")) {
                retryPut(identifier, info, retryCount + 1);
            } else {
                throw new IOException(e);
            }
        }

        LOGGER.trace("put(): wrote {} bytes to {} in bucket {} in {}",
                data.length, request.key(), request.bucket(),
                watch);
    }

    //endregion
    //region VariantCache methods

    @Override
    public void evict(final OperationList opList) {
        evict(getObjectKey(opList));
    }

    @Override
    public InputStream newVariantImageInputStream(
            OperationList opList,
            StatResult statResult) throws IOException {
        final S3Client client   = getClientInstance();
        final String bucketName = getBucket();
        final String objectKey  = getObjectKey(opList);
        LOGGER.debug("newVariantImageInputStream(): bucket: {}; key: {}",
                bucketName, objectKey);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .ifModifiedSince(earliestValidInstant())
                .build();
        try {
            ResponseInputStream<GetObjectResponse> is = client.getObject(request);
            // This extra validity check may be needed with minio server
            if (is != null && is.response().lastModified().isAfter(earliestValidInstant())) {
                statResult.setLastModified(is.response().lastModified());
                touchAsync(objectKey);
                return is;
            } else {
                IOUtils.consumeAndCloseStreamAsync(is);
                LOGGER.debug("{} in bucket {} is invalid; evicting asynchronously",
                        objectKey, bucketName);
                evictAsync(bucketName, objectKey);
            }
        } catch (S3Exception e) {
            if (e.statusCode() != 304 && e.statusCode() != 404) {
                throw new IOException(e);
            }
        } catch (SdkException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    public CompletableOutputStream
    newVariantImageOutputStream(OperationList opList) {
        final String objectKey  = getObjectKey(opList);
        final String bucketName = getBucket();
        final S3Client client   = getClientInstance();
        if (isUsingMultipartUploads()) {
            return new S3MultipartAsyncOutputStream(
                    client, opList, bucketName, objectKey, getAllObservers());
        }
        return new S3AsyncOutputStream(
                client, opList, bucketName, objectKey, getAllObservers());
    }

    //endregion

}
