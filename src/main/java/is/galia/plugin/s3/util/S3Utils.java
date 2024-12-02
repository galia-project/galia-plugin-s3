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

package is.galia.plugin.s3.util;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

/**
 * Provides some convenience methods for working with S3 buckets and objects
 * using {@link S3Client}.
 */
public final class S3Utils {

    public interface ResponseObjectHandler<T> {
        void handle(T object);
    }

    /**
     * @param client S3 client.
     * @param bucket Bucket.
     * @param key    Object key.
     * @return       Whether an object with the given key exists in the given
     *               bucket.
     */
    public static boolean objectExists(S3Client client,
                                       String bucket,
                                       String key) throws IOException {
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        HeadObjectResponse response = client.headObject(headRequest);
        int status = response.sdkHttpResponse().statusCode();
        if (status >= 400 && status < 500) {
            throw new NoSuchFileException("Received HTTP " + status + " for " + key);
        } else if (status >= 300) {
            throw new IOException("Received HTTP " + status + " for " + key);
        }
        return true;
    }

    /**
     * Invokes the given handler on all objects in the given bucket.
     *
     * @param client     S3 client.
     * @param bucketName Bucket name.
     * @param handler    Handler to invoke.
     */
    public static void walkObjects(S3Client client,
                                   String bucketName,
                                   ResponseObjectHandler<S3Object> handler) {
        walkObjects(client, bucketName, null, handler);
    }

    /**
     * Invokes the given handler on all objects in the given bucket that have
     * the given key prefix.
     *
     * @param client     S3 client.
     * @param bucketName Bucket name.
     * @param prefix     Key prefix. May be {@code null}.
     * @param handler    Handler to invoke.
     */
    public static void walkObjects(S3Client client,
                                   String bucketName,
                                   String prefix,
                                   ResponseObjectHandler<S3Object> handler) {
        String marker = null;
        ListObjectsResponse response;
        do {
            ListObjectsRequest request = ListObjectsRequest.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .marker(marker)
                    .build();
            response = client.listObjects(request);
            response.contents().forEach(handler::handle);
            marker = response.nextMarker();
        } while (response.isTruncated());
    }

    private S3Utils() {}

}
