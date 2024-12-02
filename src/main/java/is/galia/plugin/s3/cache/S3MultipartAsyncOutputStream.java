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
import is.galia.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.operation.OperationList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>Uploads written data to S3 in parts without blocking on uploads.</p>
 *
 * <p>The multi-part upload process involves three types of operations:
 * creating the upload, uploading the parts, and completing the upload. Each of
 * these are encapsulated in {@link Runnable runnable} inner classes. The
 * {@link #write} methods add appropriate instances of these to a queue which
 * is consumed by a worker running in a separate thread.</p>
 *
 * <p>Clients will notice that calls to {@link #write} and {@link #close()}
 * (that would otherwise block on communication with S3) return immediately.
 * After {@link #close()} returns, the resulting object will take a little bit
 * of time to appear in the bucket.</p>
 *
 * <p>Note that because this is a {@link CompletableOutputStream}, if the
 * instance is not {@link CompletableOutputStream#complete() marked as complete} before
 * closure, the upload will be aborted.</p>
 *
 * <p>Multi-part uploads can reduce memory usage when uploading objects larger
 * than the part length, as that is roughly the maximum amount that has to be
 * buffered in memory (provided that the length of the byte array passed to
 * either of the {@link #write} methods is not greater than the part
 * length). On the other hand, they are slower and require more requests to the
 * S3 service.</p>
 *
 * <p>N.B.: Incomplete uploads should be aborted automatically, but when using
 * Amazon S3, it may be helpful to enable the {@literal
 * AbortIncompleteMultipartUpload} lifecycle rule as a fallback.</p>
 */
final class S3MultipartAsyncOutputStream extends CompletableOutputStream {

    /**
     * The last task to perform among the other tasks in a queue.
     */
    private interface TerminalTask {}

    /**
     * Runs in a separate thread, invoking tasks provided to its work queue
     * from the various outer class methods.
     */
    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> workQueue =
                new LinkedBlockingQueue<>();
        private boolean isDone, isStopped;

        void add(Runnable task) {
            workQueue.add(task);
        }

        @Override
        public void run() {
            try {
                while (!isDone && !isStopped) {
                    try {
                        Runnable task = workQueue.take();
                        task.run();
                        if (task instanceof TerminalTask) {
                            isDone = true;
                        }
                    } catch (InterruptedException e) {
                        isStopped = true;
                    }
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

    private class RequestCreator implements Runnable {
        private final Logger logger =
                LoggerFactory.getLogger(RequestCreator.class);

        @Override
        public void run() {
            logger.trace("Creating request [bucket: {}] [key: {}]",
                    bucket, key);
            CreateMultipartUploadRequest createMultipartUploadRequest =
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(contentType)
                            .contentEncoding("UTF-8")
                            .build();
            CreateMultipartUploadResponse response =
                    client.createMultipartUpload(createMultipartUploadRequest);
            uploadID = response.uploadId();
        }
    }

    private class PartUploader implements Runnable {
        private final Logger logger =
                LoggerFactory.getLogger(PartUploader.class);

        private final ByteArrayOutputStream part;
        private final int partIndex;

        PartUploader(ByteArrayOutputStream part, int partIndex) {
            this.part      = part;
            this.partIndex = partIndex;
        }

        @Override
        public void run() {
            try {
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .uploadId(uploadID)
                        .partNumber(partIndex + 1)
                        .build();

                // There is a small chance that the last part will be empty.
                if (part.size() == 0) {
                    logger.trace("Skipping empty part {} [upload ID: {}]",
                            uploadPartRequest.partNumber(), uploadID);
                    return;
                }

                byte[] bytes = part.toByteArray();

                logger.trace("Uploading part {} ({} bytes) [upload ID: {}]",
                        uploadPartRequest.partNumber(), bytes.length, uploadID);

                String etag = client.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromBytes(bytes)).eTag();
                CompletedPart completedPart = CompletedPart.builder()
                        .partNumber(uploadPartRequest.partNumber())
                        .eTag(etag)
                        .build();
                completedParts.add(completedPart);
            } finally {
                IOUtils.closeQuietly(part);
            }
        }
    }

    private class RequestCompleter implements Runnable, TerminalTask {
        private final Logger logger =
                LoggerFactory.getLogger(RequestCompleter.class);

        @Override
        public void run() {
            try {
                logger.trace("Completing {}-part request [upload ID: {}]",
                        completedParts.size(), uploadID);

                CompletedMultipartUpload completedMultipartUpload =
                        CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build();
                CompleteMultipartUploadRequest completeMultipartUploadRequest =
                        CompleteMultipartUploadRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadID)
                                .multipartUpload(completedMultipartUpload)
                                .build();
                client.completeMultipartUpload(completeMultipartUploadRequest);

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

                complete(); // CompletableOutputStream method
                observers.forEach(o -> o.onImageWritten(opList));
            } catch (S3Exception e) {
                logger.warn(e.getMessage());
            }
        }
    }

    private class RequestAborter implements Runnable, TerminalTask {
        private final Logger logger =
                LoggerFactory.getLogger(RequestAborter.class);

        @Override
        public void run() {
            try {
                logger.trace("Aborting multipart request [upload ID: {}]",
                        uploadID);

                AbortMultipartUploadRequest abortMultipartUploadRequest =
                        AbortMultipartUploadRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadID)
                                .build();
                client.abortMultipartUpload(abortMultipartUploadRequest);
                // don't call complete()
            } catch (S3Exception e) {
                logger.warn(e.getMessage());
            }
        }
    }

    /** 5 MB is the minimum allowed by S3 for all but the last part. */
    public static final int MINIMUM_PART_LENGTH = 1024 * 1024 * 5;

    private final S3Client client;
    private final String bucket, key, contentType;

    private ByteArrayOutputStream currentPart;
    private final List<CompletedPart> completedParts = new ArrayList<>();
    private final Worker worker                      = new Worker();
    private final Set<CacheObserver> observers;
    private final OperationList opList;
    private boolean isRequestCreated;

    private String uploadID;
    private int partIndex;
    private long indexWithinPart;

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
    public S3MultipartAsyncOutputStream(S3Client client,
                                        OperationList opList,
                                        String bucket,
                                        String key,
                                        Set<CacheObserver> observers) {
        this.client      = client;
        this.opList      = opList;
        this.bucket      = bucket;
        this.key         = key;
        this.contentType = opList.getOutputFormat().getPreferredMediaType().toString();
        this.observers   = observers;
        VirtualThreadPool.getInstance().submit(worker);
    }

    @Override
    public void close() throws IOException {
        try {
            if (isComplete()) {
                worker.add(new PartUploader(getCurrentPart(), partIndex));
                // The worker will exit after running this.
                worker.add(new RequestCompleter());
            } else {
                // The worker will exit after running this.
                worker.add(new RequestAborter());
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void write(int b) {
        ByteArrayOutputStream part = getCurrentPart();
        part.write(b);
        indexWithinPart++;
        createRequestIfNecessary();
        uploadPartIfNecessary();
    }

    @Override
    public void write(byte[] b) throws IOException {
        ByteArrayOutputStream part = getCurrentPart();
        part.write(b);
        indexWithinPart += b.length;
        createRequestIfNecessary();
        uploadPartIfNecessary();
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ByteArrayOutputStream part = getCurrentPart();
        part.write(b, off, len);
        indexWithinPart += len;
        createRequestIfNecessary();
        uploadPartIfNecessary();
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

    private ByteArrayOutputStream getCurrentPart() {
        if (currentPart == null) {
            currentPart = new ByteArrayOutputStream();
        }
        return currentPart;
    }

    private void createRequestIfNecessary() {
        if (!isRequestCreated) {
            worker.add(new RequestCreator());
            isRequestCreated = true;
        }
    }

    private void uploadPartIfNecessary() {
        if (indexWithinPart >= MINIMUM_PART_LENGTH) {
            worker.add(new PartUploader(currentPart, partIndex));
            IOUtils.closeQuietly(currentPart);
            currentPart     = null;
            indexWithinPart = 0;
            partIndex++;
        }
    }

}
