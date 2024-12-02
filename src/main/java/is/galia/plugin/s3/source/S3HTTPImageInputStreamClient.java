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

import is.galia.http.MutableResponse;
import is.galia.http.Range;
import is.galia.http.Reference;
import is.galia.http.Response;
import is.galia.http.Status;
import is.galia.stream.HTTPImageInputStreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;

/**
 * Implementation backed by an AWS S3 client.
 */
class S3HTTPImageInputStreamClient implements HTTPImageInputStreamClient {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(S3HTTPImageInputStreamClient.class);

    private final S3ObjectInfo objectInfo;

    S3HTTPImageInputStreamClient(S3ObjectInfo objectInfo) {
        this.objectInfo = objectInfo;
    }

    @Override
    public Reference getReference() {
        return objectInfo.getReference();
    }

    @Override
    public Response sendHEADRequest() throws IOException {
        final S3Client client = S3Source.getClient(objectInfo);
        final String bucket   = objectInfo.getBucketName();
        final String key      = objectInfo.getKey();
        try {
            final HeadObjectResponse headResponse =
                    client.headObject(HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());
            final MutableResponse response = new MutableResponse();
            response.setStatus(Status.OK);
            response.getHeaders().set("Content-Length",
                    Long.toString(headResponse.contentLength()));
            response.getHeaders().set("Accept-Ranges", "bytes");
            return response;
        } catch (NoSuchBucketException | NoSuchKeyException e) {
            throw new NoSuchFileException(objectInfo.toString());
        } catch (S3Exception e) {
            final int code = e.statusCode();
            if (code == 403) {
                throw new AccessDeniedException(objectInfo.toString());
            } else {
                LOGGER.error(e.getMessage(), e);
                throw new IOException(e);
            }
        } catch (SdkClientException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IOException(objectInfo.toString(), e);
        }
    }

    @Override
    public Response sendGETRequest(Range range) throws IOException {
        try (InputStream is = new BufferedInputStream(
                S3Source.newObjectInputStream(objectInfo, range));
             ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final MutableResponse response = new MutableResponse();
            response.setStatus(Status.PARTIAL_CONTENT);
            is.transferTo(os);
            response.setBody(os.toByteArray());
            return response;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new IOException(objectInfo.toString(), e);
        }
    }

}
