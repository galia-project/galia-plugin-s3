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
import is.galia.delegate.Delegate;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.StatResult;
import is.galia.plugin.s3.BaseTest;
import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.test.S3SourceUtils;
import is.galia.source.Source;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class S3SourceTest extends BaseTest {

    private S3Source instance;

    @BeforeAll
    static void beforeClass() {
        new S3Source().onApplicationStart();
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
        useBasicLookupStrategy();
        instance = new S3Source();
        instance.initializePlugin();
        instance.setIdentifier(new Identifier(S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION));
    }

    void useBasicLookupStrategy() {
        Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_LOOKUP_STRATEGY.key(),
                "BasicLookupStrategy");
    }

    void useDelegateLookupStrategy() {
        Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_LOOKUP_STRATEGY.key(),
                "DelegateLookupStrategy");

        Identifier identifier = new Identifier(S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION);
        instance.setIdentifier(identifier);
        Delegate delegate = S3SourceUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
    }

    //region Plugin methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(S3Source.class.getSimpleName(),
                instance.getPluginName());
    }

    //endregion
    //region Source methods

    /* getClientInstance() */

    @Test
    void getClientInstanceReturnsDefaultClient() {
        S3ObjectInfo info = new S3ObjectInfo("", "");
        assertNotNull(S3Source.getClient(info));
    }

    @Test
    void getClientInstanceReturnsUniqueClientsPerEndpoint() {
        S3ObjectInfo info1 = new S3ObjectInfo(
                "", "http://example.org/endpoint1", "", "", "", "");
        S3ObjectInfo info2 = new S3ObjectInfo(
                "", "http://example.org/endpoint2", "", "", "", "");
        S3Client client1 = S3Source.getClient(info1);
        S3Client client2 = S3Source.getClient(info2);
        assertNotSame(client1, client2);
    }

    @Test
    void getClientInstanceCachesReturnedClients() {
        S3ObjectInfo info1 = new S3ObjectInfo(
                "", "http://example.org/endpoint", "", "", "", "");
        S3ObjectInfo info2 = new S3ObjectInfo(
                "", "http://example.org/endpoint", "", "", "", "");
        S3Client client1 = S3Source.getClient(info1);
        S3Client client2 = S3Source.getClient(info2);
        assertSame(client1, client2);
    }

    /* getFormatIterator() */

    @Test
    void getFormatIteratorConsecutiveInvocationsReturnSameInstance() {
        var it = instance.getFormatIterator();
        assertSame(it, instance.getFormatIterator());
    }

    @Test
    void getFormatIteratorHasNext() {
        instance.setIdentifier(
                new Identifier(S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION));
        Iterator<Format> it = instance.getFormatIterator();

        assertTrue(it.hasNext());
        it.next(); // object key
        assertTrue(it.hasNext());
        it.next(); // identifier extension
        assertTrue(it.hasNext());
        it.next(); // Content-Type is null
        assertTrue(it.hasNext());
        it.next(); // magic bytes
        assertFalse(it.hasNext());
    }

    @Test
    void getFormatIteratorNext() {
        instance.setIdentifier(
                new Identifier(S3SourceUtils.OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION));

        Iterator<Format> it = instance.getFormatIterator();
        assertEquals(Format.get("jpg"), it.next());     // object key
        assertEquals(Format.get("jpg"), it.next());     // identifier extension
        assertEquals(Format.UNKNOWN, it.next());        // Content-Type is null
        assertEquals(Format.get("png"), it.next());     // magic bytes
        assertThrows(NoSuchElementException.class, it::next);
    }

    /* getObjectInfo() */

    @Test
    void getObjectInfo() throws Exception {
        assertNotNull(instance.getObjectInfo());
    }

    @Test
    void getObjectInfoUsingBasicLookupStrategyWithPrefixAndSuffix()
            throws Exception {
        Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_PATH_PREFIX.key(), "prefix/");
        config.setProperty(Key.S3SOURCE_PATH_SUFFIX.key(), "/suffix");

        instance.setIdentifier(new Identifier("id"));
        assertEquals("prefix/id/suffix", instance.getObjectInfo().getKey());
    }

    @Test
    void getObjectInfoUsingBasicLookupStrategyWithoutPrefixOrSuffix()
            throws Exception {
        Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3SOURCE_PATH_PREFIX.key(), "");
        config.setProperty(Key.S3SOURCE_PATH_SUFFIX.key(), "");

        instance.setIdentifier(new Identifier("id"));
        assertEquals("id", instance.getObjectInfo().getKey());
    }

    /* newInputStream() */

    @Test
    void newInputStreamUsingBasicLookupStrategy() throws Exception {
        try (ImageInputStream is = instance.newInputStream()) {
            assertNotNull(is);
        }
    }

    @Test
    void newInputStreamUsingDelegateLookupStrategy() throws Exception {
        useDelegateLookupStrategy();
        try (ImageInputStream is = instance.newInputStream()) {
            assertNotNull(is);
        }
    }

    @Test
    void newInputStreamInvokedMultipleTimes() throws Exception {
        instance.newInputStream().close();
        instance.newInputStream().close();
        instance.newInputStream().close();
    }

    /* stat() */

    @Test
    void statUsingBasicLookupStrategyWithPresentReadableImage()
            throws Exception {
        instance.stat();
    }

    @Test
    void statUsingBasicLookupStrategyWithPresentUnreadableImage() {
        // TODO: write this
    }

    @Test
    void statUsingBasicLookupStrategyWithMissingImage() {
        instance.setIdentifier(new Identifier("bogus"));
        assertThrows(NoSuchFileException.class, instance::stat);
    }

    @Test
    void statUsingDelegateLookupStrategyWithPresentReadableImage()
            throws Exception {
        useDelegateLookupStrategy();
        instance.stat();
    }

    @Test
    void statUsingDelegateLookupStrategyWithPresentUnreadableImage() {
        // TODO: write this
    }

    @Test
    void statUsingDelegateLookupStrategyWithMissingImage() {
        useDelegateLookupStrategy();

        Identifier identifier = new Identifier("bogus");
        Delegate delegate = S3SourceUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        assertThrows(NoSuchFileException.class, () -> instance.stat());
    }

    @Test
    void statUsingDelegateLookupStrategyReturningHash() throws Exception {
        useDelegateLookupStrategy();
        final String bucket = ConfigurationService.getConfiguration()
                .getString(Key.S3SOURCE_BUCKET.key());
        Identifier identifier = new Identifier("bucket:" + bucket +
                ";key:" + S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION);
        instance.setIdentifier(identifier);

        Delegate delegate = S3SourceUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);

        instance.stat();
    }

    @Test
    void statUsingDelegateLookupStrategyWithMissingKeyInReturnedHash() {
        useDelegateLookupStrategy();

        Identifier identifier = new Identifier(
                "key:" + S3SourceUtils.OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION);
        Delegate delegate = S3SourceUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        assertThrows(IllegalArgumentException.class, () -> instance.stat());
    }

    @Test
    void statReturnsCorrectInstance() throws Exception {
        StatResult result = instance.stat();
        assertNotNull(result.getLastModified());
    }

    /**
     * Tests that {@link Source#stat()} can be invoked multiple times without
     * throwing an exception.
     */
    @Test
    void statInvokedMultipleTimes() throws Exception {
        instance.stat();
        instance.stat();
        instance.stat();
    }

}
