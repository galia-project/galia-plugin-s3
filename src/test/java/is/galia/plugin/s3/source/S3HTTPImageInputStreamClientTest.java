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

import is.galia.http.Range;
import is.galia.http.Response;
import is.galia.http.Status;
import is.galia.plugin.s3.BaseTest;
import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.test.S3SourceUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class S3HTTPImageInputStreamClientTest extends BaseTest {

    private S3HTTPImageInputStreamClient instance;

    @BeforeAll
    static void beforeClass() {
        S3SourceUtils.deleteFixtures();
        S3SourceUtils.createBucket();
        S3SourceUtils.seedFixtures();
    }

    @AfterAll
    static void afterClass() {
        S3SourceUtils.deleteBucket();
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();

        final String bucket = ConfigurationService.getConfiguration()
                .getString(Key.S3SOURCE_BUCKET.key());
        S3ObjectInfo info = new S3ObjectInfo(bucket,
                S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION);
        info.setLength(Files.size(FIXTURE));

        instance = new S3HTTPImageInputStreamClient(info);
    }

    @Test
    void getReference() {
        assertNotNull(instance.getReference());
    }

    @Test
    void sendHEADRequest() throws Exception {
        try (Response actual = instance.sendHEADRequest()) {
            assertEquals(Status.OK, actual.getStatus());
            assertEquals("bytes", actual.getHeaders().getFirstValue("Accept-Ranges"));
            assertEquals(Files.size(FIXTURE) + "",
                    actual.getHeaders().getFirstValue("Content-Length"));
        }
    }

    @Test
    void sendGETRequest() throws Exception {
        try (Response actual = instance.sendGETRequest(
                new Range(10, 50, Files.size(FIXTURE)))) {
            assertEquals(Status.PARTIAL_CONTENT, actual.getStatus());
            assertEquals(41, actual.getBody().length);
        }
    }

}
