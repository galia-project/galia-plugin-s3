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

import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.s3.BaseTest;
import is.galia.plugin.s3.test.S3CacheUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class S3MultipartAsyncOutputStreamTest extends BaseTest {

    private final String key = S3MultipartAsyncOutputStreamTest.class.getSimpleName();
    private S3MultipartAsyncOutputStream instance;

    private static OperationList newOperationList() {
        return OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
    }

    private static byte[] readBytes(String key) throws IOException {
        final String bucket = S3Cache.getBucket();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        try (ResponseInputStream<GetObjectResponse> is =
                     S3Cache.getClientInstance().getObject(request)) {
            return is.readAllBytes();
        }
    }

    @BeforeAll
    static void beforeClass() {
        S3CacheUtils.createBucket();
    }

    @AfterAll
    static void afterClass() {
        S3CacheUtils.deleteBucket();
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        S3CacheUtils.emptyBucket();
        final String bucket = S3Cache.getBucket();

        S3Client client = S3Cache.getClientInstance();
        instance = new S3MultipartAsyncOutputStream(
                client, newOperationList(), bucket, key,
                Collections.emptySet());
    }

    @Override
    @AfterEach
    public void tearDown() {
        super.tearDown();
        S3CacheUtils.emptyBucket();
    }

    @Test
    void closeMarksInstanceComplete() throws Exception {
        instance.observer = this;

        byte[] bytes = new byte[1024 * 1024];
        new SecureRandom().nextBytes(bytes);
        instance.write(bytes);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }
        assertTrue(instance.isComplete());
    }

    @Test
    void write1WithMultipleParts() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[
                S3MultipartAsyncOutputStream.MINIMUM_PART_LENGTH * 2 + 1024 * 1024];
        new SecureRandom().nextBytes(expectedBytes);

        for (byte b : expectedBytes) {
            instance.write(b);
        }
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    void write1WithSinglePart() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[1024 * 1024]; // smaller than part size
        new SecureRandom().nextBytes(expectedBytes);
        for (byte b : expectedBytes) {
            instance.write(b);
        }
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    /**
     * Tests that an object larger than the part size is written correctly.
     */
    @Test
    void write2WithMultipleParts() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[
                S3MultipartAsyncOutputStream.MINIMUM_PART_LENGTH * 2 + 1024 * 1024];
        new SecureRandom().nextBytes(expectedBytes);

        instance.write(expectedBytes);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    /**
     * Tests that an object smaller than the part size is written correctly.
     */
    @Test
    void write2WithSinglePart() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[1024 * 1024]; // smaller than part size
        new SecureRandom().nextBytes(expectedBytes);
        instance.write(expectedBytes);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    /**
     * Tests that an object larger than the part size is written correctly.
     */
    @Test
    void write3WithMultipleParts() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[
                S3MultipartAsyncOutputStream.MINIMUM_PART_LENGTH * 2 + 1024 * 1024];
        new SecureRandom().nextBytes(expectedBytes);

        instance.write(expectedBytes, 0, expectedBytes.length);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    /**
     * Tests that an object smaller than the part size is written correctly.
     */
    @Test
    void write3WithSinglePart() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[1024 * 1024]; // smaller than part size
        new SecureRandom().nextBytes(expectedBytes);
        instance.write(expectedBytes, 0, expectedBytes.length);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        byte[] actualBytes = readBytes(key);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    void closeAddsLastAccessedTimeTag() throws Exception {
        instance.observer = this;

        byte[] expectedBytes = new byte[1024 * 1024]; // smaller than part size
        new SecureRandom().nextBytes(expectedBytes);
        instance.write(expectedBytes, 0, expectedBytes.length);
        instance.complete();
        instance.close();

        synchronized (instance.lock) {
            instance.lock.wait();
        }

        S3Client client = instance.getClient();
        GetObjectTaggingRequest request = GetObjectTaggingRequest.builder()
                .bucket(instance.getBucket())
                .key(instance.getKey())
                .build();
        GetObjectTaggingResponse response = client.getObjectTagging(request);
        Tag tag = response.tagSet().getFirst();
        assertEquals(S3Cache.LAST_ACCESS_TIME_TAG_NAME, tag.key());
        assertEquals(String.valueOf(Instant.now().toEpochMilli()).length(),
                tag.value().length());
    }

}
