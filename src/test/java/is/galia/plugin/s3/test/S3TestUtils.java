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

package is.galia.plugin.s3.test;

import is.galia.plugin.s3.util.S3Utils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

public final class S3TestUtils {

    private S3TestUtils() {}

    /**
     * @param client S3 client.
     * @param bucket Bucket.
     * @return       Whether an object with the given key exists in the given
     *               bucket.
     */
    private static boolean bucketExists(S3Client client,
                                        String bucket) {
        HeadBucketRequest headRequest = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            HeadBucketResponse response = client.headBucket(headRequest);
            response.sdkHttpResponse().statusCode();
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    /**
     * Creates the given bucket. If it already exists, it is emptied out.
     *
     * @param client     S3 client.
     * @param bucketName Bucket name.
     */
    static void createBucket(S3Client client, String bucketName) {
        if (!bucketExists(client, bucketName)) {
            client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName).build());
        }
    }

    /**
     * Empties the given bucket and then deletes it.
     *
     * @param client     S3 client.
     * @param bucketName Bucket to empty out and delete.
     */
    static void deleteBucket(S3Client client, String bucketName) {
        if (bucketExists(client, bucketName)) {
            emptyBucket(client, bucketName);
            client.deleteBucket(DeleteBucketRequest.builder()
                    .bucket(bucketName).build());
        }
    }

    /**
     * @param client     S3 client.
     * @param bucketName Name of the bucket to empty out.
     */
    static void emptyBucket(S3Client client, String bucketName) {
        if (bucketExists(client, bucketName)) {
            S3Utils.walkObjects(client, bucketName, null, (object) -> {
                client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(object.key())
                        .build());
            });
        }
    }

}
