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

import is.galia.config.Configuration;
import is.galia.plugin.s3.BaseTest;
import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.test.S3SourceUtils;
import is.galia.stream.ClosingFileCacheImageInputStream;
import is.galia.stream.HTTPImageInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class S3StreamFactoryTest extends BaseTest {

    private S3StreamFactory instance;

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

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        final String bucket = ConfigurationService.getConfiguration()
                .getString(Key.S3SOURCE_BUCKET.key());
        S3ObjectInfo info = new S3ObjectInfo(bucket, "ghost1.png");
        info.setLength(Files.size(FIXTURE));

        instance = new S3StreamFactory(info);
    }

    @Test
    void newSeekableStream() throws Exception {
        int length = 0;
        try (ImageInputStream is = instance.newSeekableStream()) {
            while (is.read() != -1) {
                length++;
            }
        }
        assertEquals(Files.size(FIXTURE), length);
    }

    @Test
    void newSeekableStreamClassWithChunkingEnabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_CHUNKING_ENABLED.key(), true);
        config.setProperty(Key.S3SOURCE_CHUNK_SIZE.key(), "777K");

        try (ImageInputStream is = instance.newSeekableStream()) {
            assertInstanceOf(HTTPImageInputStream.class, is);
            assertEquals(777 * 1024, ((HTTPImageInputStream) is).getWindowSize());
        }
    }

    @Test
    void newSeekableStreamClassWithChunkingDisabled() throws Exception {
        Configuration.forApplication().setProperty(Key.S3SOURCE_CHUNKING_ENABLED.key(), false);
        try (ImageInputStream is = instance.newSeekableStream()) {
            assertInstanceOf(ClosingFileCacheImageInputStream.class, is);
        }
    }

    @Test
    void newSeekableStreamWithChunkingEnabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_CHUNKING_ENABLED.key(), true);
        config.setProperty(Key.S3SOURCE_CHUNK_SIZE.key(), "777K");

        try (ImageInputStream is = instance.newSeekableStream()) {
            assertInstanceOf(HTTPImageInputStream.class, is);
            HTTPImageInputStream htis = (HTTPImageInputStream) is;
            assertEquals(777 * 1024, htis.getWindowSize());
        }
    }

}
