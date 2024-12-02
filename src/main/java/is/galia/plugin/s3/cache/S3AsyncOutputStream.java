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

import is.galia.async.ThreadPool;
import is.galia.async.VirtualThreadPool;
import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.operation.OperationList;
import is.galia.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

/**
 * <p>Wraps a {@link ByteArrayOutputStream} for upload to S3.</p>
 *
 * <p>N.B.: S3 does not allow uploads without a {@code Content-Length}
 * header, which cannot be provided when streaming an unknown amount of
 * data (which this class is going to be doing all the time). From the
 * documentation of {@link PutObjectRequest}:</p>
 *
 * <blockquote>"When uploading directly from an input stream, content
 * length must be specified before data can be uploaded to Amazon S3. If
 * not provided, the library will have to buffer the contents of the input
 * stream in order to calculate it. Amazon S3 explicitly requires that the
 * content length be sent in the request headers before any of the data is
 * sent."</blockquote>
 *
 * <p>Since it's not possible to stream data of unknown length to the S3, this
 * class buffers written data in a byte array before uploading it to S3
 * upon closure. (The upload is submitted to the
 * {@link ThreadPool#getInstance() application thread pool} in order for
 * {@link #close()} to be able to return immediately.)</p>
 */
class S3AsyncOutputStream extends CompletableOutputStream {

    private class S3Upload implements Runnable {

        private static final Logger UPLOAD_LOGGER =
                LoggerFactory.getLogger(S3Upload.class);

        private final String bucketName, contentEncoding, contentType, objectKey;
        private final byte[] data;
        private final S3Client client;

        /**
         * @param client          S3 client.
         * @param data            Data to upload.
         * @param bucketName      S3 bucket name.
         * @param objectKey       S3 object key.
         * @param contentType     Media type.
         * @param contentEncoding Content encoding. May be {@code null}.
         */
        S3Upload(S3Client client,
                 byte[] data,
                 String bucketName,
                 String objectKey,
                 String contentType,
                 String contentEncoding) {
            this.client          = client;
            this.bucketName      = bucketName;
            this.data            = data;
            this.contentType     = contentType;
            this.contentEncoding = contentEncoding;
            this.objectKey       = objectKey;
        }

        @Override
        public void run() {
            try {
                if (data.length > 0) {
                    PutObjectRequest request = PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .contentType(contentType)
                            .contentEncoding(contentEncoding)
                            .build();
                    final Stopwatch watch = new Stopwatch();

                    UPLOAD_LOGGER.debug("Uploading {} bytes to {} in bucket {}",
                            data.length, request.key(), request.bucket());

                    try (ByteArrayInputStream is = new ByteArrayInputStream(data)) {
                        client.putObject(request,
                                RequestBody.fromInputStream(is, data.length));
                    } catch (IOException e) {
                        UPLOAD_LOGGER.warn(e.getMessage(), e);
                    }

                    // Add a last-accessed time tag.
                    Tagging tagging = Tagging.builder()
                            .tagSet(S3Cache.newLastAccessedTag())
                            .build();
                    PutObjectTaggingRequest taggingRequest = PutObjectTaggingRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .tagging(tagging)
                            .build();
                    client.putObjectTagging(taggingRequest);

                    observers.forEach(o -> o.onImageWritten(opList));

                    UPLOAD_LOGGER.trace("Wrote {} bytes to {} in bucket {} in {}",
                            data.length, request.key(), request.bucket(),
                            watch);
                } else {
                    UPLOAD_LOGGER.trace("No data to upload; returning");
                }
            } finally {
                if (observer != null) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        }

    }

    private final S3Client client;
    private final String bucket, key, contentType;

    /** Buffers written data. */
    private final ByteArrayOutputStream bufferStream =
            new ByteArrayOutputStream();

    private final OperationList opList;
    private final Set<CacheObserver> observers;

    /** For an instance to wait for an upload notification during testing. */
    Object observer;

    /** Helps notify {@link #observer} of a completed upload during testing. */
    final Object lock = new Object();

    /**
     * @param client    Client.
     * @param opList    Instance describing the image being written.
     * @param bucket    Target bucket.
     * @param key       Target key.
     * @param observers Observers of the cache utilizing this instance.
     */
    S3AsyncOutputStream(S3Client client,
                        OperationList opList,
                        String bucket,
                        String key,
                        Set<CacheObserver> observers) {
        this.client      = client;
        this.opList      = opList;
        this.bucket = bucket;
        this.key = key;
        this.contentType = opList.getOutputFormat().getPreferredMediaType().toString();
        this.observers   = observers;
    }

    @Override
    public void close() throws IOException {
        try {
            bufferStream.close();
            byte[] data = bufferStream.toByteArray();
            if (isComplete()) {
                // At this point, the client has received all image data, but
                // it is still waiting on us to close the connection.
                // Uploading in a separate thread will allow this to happen
                // immediately.
                VirtualThreadPool.getInstance().submit(new S3Upload(
                        client, data, bucket, key,
                        contentType, null));
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void flush() throws IOException {
        bufferStream.flush();
    }

    @Override
    public void write(int b) {
        bufferStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        bufferStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        bufferStream.write(b, off, len);
    }

    S3Client getClient() {
        return client;
    }

    String getBucket() {
        return bucket;
    }

    String getKey() {
        return key;
    }

}
