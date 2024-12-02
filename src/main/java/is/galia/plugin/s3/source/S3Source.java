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

package is.galia.plugin.s3.source;

import is.galia.codec.FormatDetector;
import is.galia.delegate.DelegateException;
import is.galia.plugin.Plugin;
import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.util.S3ClientBuilder;
import is.galia.stream.ClosingMemoryCacheImageInputStream;
import is.galia.stream.HTTPImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import is.galia.config.Configuration;
import is.galia.http.Range;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.MediaType;
import is.galia.image.StatResult;
import is.galia.source.AbstractSource;
import is.galia.source.FormatChecker;
import is.galia.source.IdentifierFormatChecker;
import is.galia.source.LookupStrategy;
import is.galia.source.NameFormatChecker;
import is.galia.source.Source;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Maps an identifier to an <a href="https://aws.amazon.com/s3/">Amazon
 * Simple Storage Service (S3)</a> object, for retrieving images from S3.</p>
 *
 * <h1>Format Inference</h1>
 *
 * <p>See {@link FormatIterator}.</p>
 *
 * <h1>Lookup Strategies</h1>
 *
 * <p>Two distinct lookup strategies are supported, defined by
 * {@link Key#S3SOURCE_LOOKUP_STRATEGY}. BasicLookupStrategy maps
 * identifiers directly to S3 object keys. DelegateLookupStrategy invokes a
 * delegate method to retrieve object keys dynamically.</p>
 *
 * <h1>Resource Access</h1>
 *
 * <p>While proceeding through the client request fulfillment flow, the
 * following server requests are sent:</p>
 *
 * <ol>
 *     <li>{@literal HEAD}</li>
 *     <li>
 *         <ol>
 *             <li>If {@link FormatIterator#next()} needs to check magic bytes:
 *                 <ol>
 *                     <li>Ranged {@literal GET}</li>
 *                 </ol>
 *             </li>
 *             <li>If chunking is enabled:
 *                 <ol>
 *                     <li>A series of ranged {@literal GET} requests (see {@link
 *                     HTTPImageInputStream} for
 *                     details)</li>
 *                 </ol>
 *             </li>
 *             <li>Else if chunking is not enabled:
 *                 <ol>
 *                     <li>{@literal GET} to retrieve the full image bytes</li>
 *                 </ol>
 *             </li>
 *         </ol>
 *     </li>
 * </ol>
 */
public final class S3Source extends AbstractSource implements Source, Plugin {

    private static class S3ObjectAttributes {
        String contentType;
        Instant lastModified;
        long length;
    }

    /**
     * <ol>
     *     <li>If the object key has a recognized filename extension, the
     *     format is inferred from that.</li>
     *     <li>Otherwise, if the source image's URI identifier has a recognized
     *     filename extension, the format will be inferred from that.</li>
     *     <li>Otherwise, a {@literal GET} request will be sent with a
     *     {@literal Range} header specifying a small range of data from the
     *     beginning of the resource.
     *         <ol>
     *             <li>If a {@literal Content-Type} header is present in the
     *             response, and is specific enough (i.e. not {@literal
     *             application/octet-stream}), a format will be inferred from
     *             that.</li>
     *             <li>Otherwise, a format is inferred from the magic bytes in
     *             the response body.</li>
     *         </ol>
     *     </li>
     * </ol>
     *
     * @param <T> {@link Format}.
     */
    class FormatIterator<T> implements Iterator<T> {

        /**
         * Infers a {@link Format} based on the media type in a {@literal
         * Content-Type} header.
         */
        private class ContentTypeHeaderChecker implements FormatChecker {
            @Override
            public Format check() throws IOException {
                String contentType = getObjectAttributes().contentType;
                if (contentType != null && !contentType.isEmpty()) {
                    return MediaType.fromString(contentType).toFormat();
                }
                return Format.UNKNOWN;
            }
        }

        /**
         * Infers a {@link Format} based on image magic bytes.
         */
        private class ByteChecker implements FormatChecker {
            @Override
            public Format check() throws IOException {
                Range range = new Range(0, FormatDetector.RECOMMENDED_READ_LENGTH);
                try (ImageInputStream is = new ClosingMemoryCacheImageInputStream(
                        newObjectInputStream(getObjectInfo(), range))) {
                    return FormatDetector.detect(is);
                }
            }
        }

        private FormatChecker formatChecker;

        @Override
        public boolean hasNext() {
            return (formatChecker == null ||
                    formatChecker instanceof NameFormatChecker ||
                    formatChecker instanceof IdentifierFormatChecker ||
                    formatChecker instanceof FormatIterator.ContentTypeHeaderChecker);
        }

        @Override
        public T next() {
            if (formatChecker == null) {
                try {
                    formatChecker = new NameFormatChecker(getObjectInfo().getKey());
                } catch (IOException e) {
                    LOGGER.warn("FormatIterator.next(): {}", e.getMessage(), e);
                    formatChecker = new NameFormatChecker("***BOGUS***");
                    return next();
                }
            } else if (formatChecker instanceof NameFormatChecker) {
                formatChecker = new IdentifierFormatChecker(getIdentifier());
            } else if (formatChecker instanceof IdentifierFormatChecker) {
                formatChecker = new ContentTypeHeaderChecker();
            } else if (formatChecker instanceof FormatIterator.ContentTypeHeaderChecker) {
                formatChecker = new ByteChecker();
            } else {
                throw new NoSuchElementException();
            }
            try {
                //noinspection unchecked
                return (T) formatChecker.check();
            } catch (IOException e) {
                LOGGER.warn("Error checking format: {}", e.getMessage());
                //noinspection unchecked
                return (T) Format.UNKNOWN;
            }
        }
    }

    private static final Logger LOGGER =
            LoggerFactory.getLogger(S3Source.class);

    private static final String DEFAULT_ENDPOINT_KEY = "default";

    /**
     * The keys are either endpoint URIs or {@link #DEFAULT_ENDPOINT_KEY}.
     * This is not thread-safe, so it should only be accessed via {@link
     * #getClient(S3ObjectInfo)}.
     */
    private static final Map<String,S3Client> CLIENTS = new HashMap<>();

    private static final String OBJECT_INFO_DELEGATE_METHOD =
            "s3source_object_info";

    /**
     * Cached by {@link #getObjectInfo()}.
     */
    private S3ObjectInfo objectInfo;

    /**
     * Cached by {@link #getObjectAttributes()}.
     */
    private S3ObjectAttributes objectAttributes;

    private FormatIterator<Format> formatIterator = new FormatIterator<>();

    static synchronized S3Client getClient(S3ObjectInfo info) {
        String endpoint = info.getEndpoint();
        final String endpointKey =
                (endpoint != null) ? endpoint : DEFAULT_ENDPOINT_KEY;
        S3Client client = CLIENTS.get(endpointKey);
        if (client == null) {
            final Configuration config = Configuration.forApplication();
            if (endpoint == null) {
                endpoint = config.getString(Key.S3SOURCE_ENDPOINT.key());
            }
            // Convert the endpoint string into a URI which is required by the
            // client builder.
            URI endpointURI = null;
            if (endpoint != null) {
                try {
                    endpointURI = new URI(endpoint);
                } catch (URISyntaxException e) {
                    LOGGER.error("Invalid URI for {}: {}",
                            Key.S3SOURCE_ENDPOINT, e.getMessage());
                }
            }
            String region = info.getRegion();
            if (region == null) {
                region = config.getString(Key.S3SOURCE_REGION.key());
            }
            String accessKeyID = info.getAccessKeyID();
            if (accessKeyID == null) {
                accessKeyID = config.getString(Key.S3SOURCE_ACCESS_KEY_ID.key());
            }
            String secretAccessKey = info.getSecretAccessKey();
            if (secretAccessKey == null) {
                secretAccessKey = config.getString(Key.S3SOURCE_SECRET_ACCESS_KEY.key());
            }
            client = new S3ClientBuilder()
                    .accessKeyID(accessKeyID)
                    .secretAccessKey(secretAccessKey)
                    .endpointURI(endpointURI)
                    .region(region)
                    .asyncCredentialUpdateEnabled(
                            config.getBoolean(Key.S3SOURCE_ASYNC_CREDENTIAL_UPDATE.key(), true))
                    .build();
            CLIENTS.put(endpointKey, client);
        }
        return client;
    }

    /**
     * Fetches a whole object.
     *
     * @param info Object info.
     */
    static InputStream newObjectInputStream(S3ObjectInfo info)
            throws IOException {
        return newObjectInputStream(info, null);
    }

    /**
     * Fetches a byte range of an object.
     *
     * @param info  Object info.
     * @param range Byte range. May be {@code null}.
     */
    static InputStream newObjectInputStream(S3ObjectInfo info,
                                            Range range) throws IOException {
        final S3Client client = getClient(info);
        try {
            GetObjectRequest request;
            if (range != null) {
                LOGGER.debug("Requesting bytes {}-{} from {}",
                        range.start(), range.end(), info);
                request = GetObjectRequest.builder()
                        .bucket(info.getBucketName())
                        .key(info.getKey())
                        .range("bytes=" + range.start() + "-" + range.end())
                        .build();
            } else {
                LOGGER.debug("Requesting {}", info);
                request = GetObjectRequest.builder()
                        .bucket(info.getBucketName())
                        .key(info.getKey())
                        .build();
            }
            return client.getObject(request);
        } catch (NoSuchBucketException | NoSuchKeyException e) {
            throw new NoSuchFileException(info.toString());
        } catch (SdkException e) {
            throw new IOException(info.toString(), e);
        }
    }

    private S3ObjectAttributes getObjectAttributes() throws IOException {
        if (objectAttributes == null) {
            // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
            final S3ObjectInfo info = getObjectInfo();
            final String bucket     = info.getBucketName();
            final String key        = info.getKey();
            final S3Client client   = getClient(info);
            try {
                HeadObjectResponse response = client.headObject(HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build());
                objectAttributes              = new S3ObjectAttributes();
                objectAttributes.length       = response.contentLength();
                objectAttributes.lastModified = response.lastModified();
            } catch (NoSuchBucketException | NoSuchKeyException e) {
                throw new NoSuchFileException(info.toString());
            } catch (S3Exception e) {
                final int code = e.statusCode();
                if (code == 403) {
                    throw new AccessDeniedException(info.toString());
                } else {
                    LOGGER.error(e.getMessage(), e);
                    throw new IOException(e);
                }
            } catch (SdkClientException e) {
                LOGGER.error(e.getMessage(), e);
                throw new IOException(info.toString(), e);
            }
        }
        return objectAttributes;
    }

    /**
     * @return Info for the current object from either the configuration or the
     *         delegate system. The {@link S3ObjectInfo#getLength() length} is
     *         not set. The result is cached.
     */
    S3ObjectInfo getObjectInfo() throws IOException {
        if (objectInfo == null) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (LookupStrategy.from(Key.S3SOURCE_LOOKUP_STRATEGY.key())) {
                case DELEGATE_SCRIPT:
                    try {
                        objectInfo = getObjectInfoUsingDelegateStrategy();
                    } catch (DelegateException e) {
                        throw new IOException(e);
                    }
                    break;
                default:
                    objectInfo = getObjectInfoUsingBasicStrategy();
                    break;
            }
        }
        return objectInfo;
    }

    /**
     * @return Object info based on {@link #identifier} and the application
     *         configuration.
     */
    private S3ObjectInfo getObjectInfoUsingBasicStrategy() {
        final var config        = Configuration.forApplication();
        final String bucketName = config.getString(Key.S3SOURCE_BUCKET.key());
        final String keyPrefix  = config.getString(Key.S3SOURCE_PATH_PREFIX.key(), "");
        final String keySuffix  = config.getString(Key.S3SOURCE_PATH_SUFFIX.key(), "");
        final String key        = keyPrefix + identifier.toString() + keySuffix;
        return new S3ObjectInfo(bucketName, key);
    }

    /**
     * @return Object info drawn from the {@link #OBJECT_INFO_DELEGATE_METHOD}.
     * @throws IllegalArgumentException if the return value of the delegate
     *                                  method is invalid.
     * @throws NoSuchFileException      if the delegate script does not exist.
     * @throws DelegateException        if the delegate method throws an
     *                                  exception.
     */
    private S3ObjectInfo getObjectInfoUsingDelegateStrategy()
            throws DelegateException, NoSuchFileException {
        @SuppressWarnings("unchecked")
        final Map<String,String> result = (Map<String,String>) getDelegate().invoke(
                OBJECT_INFO_DELEGATE_METHOD);
        if (result == null || result.isEmpty()) {
            throw new NoSuchFileException(
                    OBJECT_INFO_DELEGATE_METHOD +
                            " returned nil for " + identifier);
        } else if (result.containsKey("bucket") && result.containsKey("key")) {
            return new S3ObjectInfo(
                    result.get("region"),
                    result.get("endpoint"),
                    result.get("access_key_id"),
                    result.get("secret_access_key"),
                    result.get("bucket"),
                    result.get("key"));
        } else {
            throw new IllegalArgumentException(
                    "Returned hash must include bucket and key");
        }
    }

    private void reset() {
        objectInfo       = null;
        objectAttributes = null;
        formatIterator   = new FormatIterator<>();
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(S3Source.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return getClass().getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void onApplicationStop() {}

    @Override
    public void initializePlugin() {}

    //endregion
    //region Source methods

    @Override
    public StatResult stat() throws IOException {
        S3ObjectAttributes attrs = getObjectAttributes();
        StatResult result = new StatResult();
        result.setLastModified(attrs.lastModified);
        return result;
    }

    @Override
    public Iterator<Format> getFormatIterator() {
        return formatIterator;
    }

    @Override
    public ImageInputStream newInputStream() throws IOException {
        S3ObjectInfo info = getObjectInfo();
        info.setLength(getObjectAttributes().length);
        return new S3StreamFactory(info).newSeekableStream();
    }

    @Override
    public void setIdentifier(Identifier identifier) {
        super.setIdentifier(identifier);
        reset();
    }

}
