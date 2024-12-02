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

import is.galia.http.Reference;

/**
 * Contains information needed to access an object in S3.
 */
final class S3ObjectInfo {

    private final String region, endpoint, accessKeyID, secretAccessKey,
            bucketName, key;
    private long length = -1;

    /**
     * Basic constructor for use with static (or {@code null}) region,
     * endpoint, and credentials.
     */
    S3ObjectInfo(String bucketName, String key) {
        this(null, null, null, null, bucketName, key);
    }

    /**
     * Constructor utilizing detailed information returned from a delegate
     * method.
     */
    S3ObjectInfo(String region, String endpoint, String accessKeyID,
                 String secretAccessKey, String bucketName, String key) {
        this.region          = region;
        this.endpoint        = endpoint;
        this.accessKeyID     = accessKeyID;
        this.secretAccessKey = secretAccessKey;
        this.bucketName      = bucketName;
        this.key             = key;
    }

    /**
     * @return Access key ID. May be {@code null}.
     */
    String getAccessKeyID() {
        return accessKeyID;
    }

    String getBucketName() {
        return bucketName;
    }

    /**
     * @return Service endpoint URI. May be {@code null}.
     */
    String getEndpoint() {
        return endpoint;
    }

    String getKey() {
        return key;
    }

    long getLength() {
        return length;
    }

    /**
     * @return Endpoint AWS region. Only used by AWS endpoints. May be {@code
     *         null}.
     */
    String getRegion() {
        return region;
    }

    /**
     * @return S3 URI.
     */
    Reference getReference() {
        StringBuilder uri = new StringBuilder();
        uri.append("s3://");
        if (endpoint != null) {
            uri.append(endpoint);
            uri.append("/");
        }
        uri.append(bucketName);
        uri.append("/");
        uri.append(key);
        return new Reference(uri.toString());
    }

    /**
     * @return Secret access key. May be {@code null}.
     */
    String getSecretAccessKey() {
        return secretAccessKey;
    }

    void setLength(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[endpoint: ").append(getEndpoint()).append("] ");
        b.append("[region: ").append(getRegion()).append("] ");
        String tmp = getAccessKeyID() != null ? "******" : "null";
        b.append("[accessKeyID: ").append(tmp).append("] ");
        tmp = getSecretAccessKey() != null ? "******" : "null";
        b.append("[secretAccessKey: ").append(tmp).append("] ");
        b.append("[bucket: ").append(getBucketName()).append("] ");
        b.append("[key: ").append(getKey()).append("]");
        return b.toString();
    }

}
