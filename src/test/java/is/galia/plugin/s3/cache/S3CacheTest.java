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

import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.config.Configuration;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.StatResult;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.s3.BaseTest;
import is.galia.plugin.s3.test.Assert;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.test.S3CacheUtils;
import is.galia.plugin.s3.util.S3Utils;
import is.galia.plugin.s3.config.Key;
import is.galia.util.ConcurrentProducerConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static is.galia.config.Key.VARIANT_CACHE_TTL;

class S3CacheTest extends BaseTest {

    private static final int ASYNC_WAIT = 3000;

    private final OperationList opList = new OperationList();
    private S3Cache instance;

    @BeforeAll
    static void beforeClass() {
        new S3Cache().onApplicationStart();
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

        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 300);
        instance = new S3Cache();
        instance.initializePlugin();
    }

    @Override
    @AfterEach
    public void tearDown() {
        super.tearDown();
        S3CacheUtils.emptyBucket();
    }

    //region Plugin methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(S3Cache.class.getSimpleName(), instance.getPluginName());
    }

    //endregion
    //region Cache methods

    /* evict(Identifier) */

    @Test
    void evict() throws Exception {
        // add an image and an info
        final Identifier id1        = new Identifier("cats");
        final OperationList opList1 = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id1, new Info());

        // add another image and another info
        final Identifier id2        = new Identifier("dogs");
        final OperationList opList2 = OperationList.builder()
                .withIdentifier(id2)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id2, new Info());

        assertNotNull(instance.fetchInfo(id1));
        assertNotNull(instance.fetchInfo(id2));

        // evict one of the info/image pairs
        instance.evict(id1);

        // assert that its info and image are gone
        assertFalse(instance.fetchInfo(id1).isPresent());

        try (InputStream is = instance.newVariantImageInputStream(opList1)) {
            assertNull(is);
        }

        // ... but the other one is still there
        assertNotNull(instance.fetchInfo(id2));
        try (InputStream is = instance.newVariantImageInputStream(opList2)) {
            assertNotNull(is);
        }
    }

    /* evict(OperationList) */

    @Test
    void evictWithOperationList() throws Exception {
        // Seed a variant image
        OperationList ops1 = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // Seed another variant image
        OperationList ops2 = OperationList.builder()
                .withIdentifier(new Identifier("dogs"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        Thread.sleep(ASYNC_WAIT);

        // Evict the first one
        instance.evict(ops1);

        // Assert that it was evicted
        Assert.assertNotExists(instance, ops1);

        // Assert that the other one was NOT evicted
        Assert.assertExists(instance, ops2);
    }

    /* evictInfos() */

    @Test
    void evictInfos() throws Exception {
        Identifier identifier = new Identifier(FIXTURE.getFileName().toString());
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info             = new Info();

        // add an image
        final CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }
        // add an info
        instance.put(identifier, info);

        latch.await();

        // assert that they've been added
        Assert.assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // Evict infos
        instance.evictInfos();

        // assert that the info has been evicted
        //final AtomicInteger infoCounter = new AtomicInteger();
        //S3Utils.walkObjects(S3Cache.getClientInstance(), instance.getBucket(),
        //        S3Cache.INFO_KEY_PREFIX,
        //       (object) -> infoCounter.incrementAndGet());
        //assertEquals(0, infoCounter.get());

        // assert that the image has NOT been evicted
        final AtomicInteger imageCounter = new AtomicInteger();
        S3Utils.walkObjects(S3Cache.getClientInstance(), instance.getBucket(),
                (object) -> imageCounter.incrementAndGet());
        assertEquals(1, imageCounter.get());
    }

    /* evictInvalid() */

    @Test
    void evictInvalid() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 3);

        // add an image
        Identifier id1     = new Identifier("id1");
        OperationList ops1 = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info1         = new Info();

        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add an Info
        instance.put(id1, info1);

        // wait for them to invalidate
        Thread.sleep(3100);

        // add another image
        Identifier id2     = new Identifier("cats");
        OperationList ops2 = OperationList.builder()
                .withIdentifier(new Identifier("id2"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info2                 = new Info();
        final CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops2)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }
        latch.await(10, TimeUnit.SECONDS);

        // add another info
        instance.put(id2, info2);

        instance.evictInvalid();

        // Assert that one image and one info have been evicted.
        // N.B. Any of these failing is probably a TTL timing sensitivity
        // problem.
        assertFalse(instance.fetchInfo(id1).isPresent());
        assertTrue(instance.fetchInfo(id2).isPresent());
        Assert.assertNotExists(instance, ops1);
        Assert.assertExists(instance, ops2);
    }

    @Test
    void evictInvalidWithKeyPrefix() throws Exception {
        final String prefix        = "prefix/";
        final Configuration config = Configuration.forApplication();
        config.setProperty(VARIANT_CACHE_TTL.key(), 2);
        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), prefix);

        // Add a random file outside the key prefix, which will be allowed to
        // "expire" as if it were cached. This test will assert that it still
        // exists after purging invalid content.
        final S3Client client         = S3Cache.getClientInstance();
        final String bucket           = ConfigurationService.getConfiguration()
                .getString(Key.S3CACHE_BUCKET.key());
        final String keyOutsidePrefix = "some-key";
        final byte[] data             = "some data".getBytes(StandardCharsets.UTF_8);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(keyOutsidePrefix)
                .build();
        try (ByteArrayInputStream is = new ByteArrayInputStream(data)) {
            client.putObject(request,
                    RequestBody.fromInputStream(is, data.length));
        }

        // add a cached variant image
        Identifier id1        = new Identifier(FIXTURE.getFileName().toString());
        OperationList ops1    = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        final CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add a cached Info
        Info info1 = new Info();
        instance.put(id1, info1);

        latch.await(10, TimeUnit.SECONDS);

        // assert that they've been added
        assertNotNull(instance.fetchInfo(id1));
        Assert.assertExists(instance, ops1);

        // wait for them to invalidate
        Thread.sleep(ASYNC_WAIT);

        instance.evictInvalid();

        // assert that the image and info have been evicted
        assertFalse(instance.fetchInfo(id1).isPresent());
        Assert.assertNotExists(instance, ops1);

        // assert that the object outside the cache prefix has NOT been evicted
        assertTrue(S3Utils.objectExists(client, bucket, keyOutsidePrefix));
    }

    /* fetchInfo(Identifier) */

    @Test
    void fetchInfoWithExistingValidImage() throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info = new Info();
        instance.put(identifier, info);

        Optional<Info> actual = instance.fetchInfo(identifier);
        assertEquals(actual.orElseThrow(), info);
    }

    @Test
    void fetchInfoWithExistingInvalidImage() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 1);

        Identifier identifier = new Identifier("cats");
        Info info             = new Info();
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        assertFalse(instance.fetchInfo(identifier).isPresent());
    }

    @Test
    void fetchInfoWithNonexistentImage() throws Exception {
        assertFalse(instance.fetchInfo(new Identifier("bogus")).isPresent());
    }

    @Test
    void fetchInfoUpdatesLastModifiedTime() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 1);

        Identifier identifier = new Identifier("cats");
        Info info = new Info();
        instance.put(identifier, info);

        for (int i = 0; i < 10; i++) {
            Thread.sleep(250);
            assertNotNull(instance.fetchInfo(identifier));
        }
    }

    @Test
    void fetchInfoPopulatesSerializationTimestampWhenNotAlreadySet()
            throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info             = new Info();
        instance.put(identifier, info);

        info = instance.fetchInfo(identifier).orElseThrow();
        assertNotNull(info.getSerializationTimestamp());
    }

    @Test
    void fetchInfoConcurrently() {
        // This is tested by putConcurrently()
    }

    /* getBucket() */

    @Test
    void getBucket() {
        assertEquals(
                Configuration.forApplication().getString(Key.S3CACHE_BUCKET.key()),
                instance.getBucket());
    }

    /* getObjectKey(Identifier) */

    @Test
    void getObjectKeyWithIdentifier() {
        String name = instance.getObjectKey(new Identifier("cats"));
        assertTrue(name.matches(
                "^" + instance.getObjectKeyPrefix() + "info/[a-z0-9]{32}.json$"));
    }

    /* getObjectKey(OperationList */

    @Test
    void getObjectKeyWithOperationList() {
        opList.setIdentifier(new Identifier("cats"));
        String name = instance.getObjectKey(opList);
        assertTrue(name.matches("^" + instance.getObjectKeyPrefix() + "image/[a-z0-9]{32}/[a-z0-9]{32}$"));
    }

    /* getObjectKeyPrefix() */

    @Test
    void getObjectKeyPrefix() {
        Configuration config = Configuration.forApplication();

        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "");
        assertEquals("", instance.getObjectKeyPrefix());

        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "/");
        assertEquals("", instance.getObjectKeyPrefix());

        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "cats");
        assertEquals("cats/", instance.getObjectKeyPrefix());

        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "cats/");
        assertEquals("cats/", instance.getObjectKeyPrefix());
    }

    /* newVariantImageInputStream(OperationList) */

    @Test
    void newVariantImageInputStreamWithZeroTTL() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 0);

        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in and assert same size
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageInputStreamWithNonzeroTTL() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 2);

        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image. (The write may complete before data is fully or even
        // partially written to the cache.)
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Assert that it has been added.
        Assert.assertExists(instance, opList);
        // Wait for it to invalidate.
        Thread.sleep(3000);
        // Assert that it has been evicted.
        Assert.assertNotExists(instance, opList);
    }

    @Test
    void newVariantImageInputStreamWithNonexistentImage() {
        final OperationList ops = new OperationList(new Identifier("cats"));
        instance.purge();
        Assert.assertNotExists(instance, ops);
    }

    @Test
    void newVariantImageInputStreamConcurrently() throws Exception {
        final OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        new ConcurrentProducerConsumer(() -> {
            try (CompletableOutputStream os =
                         instance.newVariantImageOutputStream(ops)) {
                Files.copy(FIXTURE, os);
                os.complete();
            }
            return null;
        }, () -> {
            try (InputStream is = instance.newVariantImageInputStream(ops)) {
                if (is != null) {
                    //noinspection StatementWithEmptyBody
                    while (is.read() != -1) {
                        // consume the stream fully
                    }
                }
            }
            return null;
        }).run();
    }

    /* newVariantImageInputStream(OperationList, StatResult) */

    @Test
    void newVariantImageInputStreamPopulatesStatResult() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 0);
        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        StatResult statResult = new StatResult();
        try (InputStream is = instance.newVariantImageInputStream(opList, statResult)) {
            assertNotNull(statResult.getLastModified());
            is.readAllBytes();
        }
    }

    /* newVariantImageOutputStream() */

    @Test
    void newVariantImageOutputStream() throws Exception {
        OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageOutputStreamDoesNotLeaveDetritusWhenStreamIsIncompletelyWritten()
            throws Exception {
        OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            // don't set it complete
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Try to read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            assertNull(is);
        }
    }

    @Test
    void newVariantImageOutputStreamConcurrently() {
        // This is tested in testNewVariantImageInputStreamConcurrently()
    }

    @Test
    void newVariantImageOutputStreamOverwritesExistingImage() {
        // TODO: write this
    }

    /* purge() */

    @Test
    void purge() throws Exception {
        Identifier identifier = new Identifier(FIXTURE.getFileName().toString());
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info            = new Info();

        final CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        // add an info
        instance.put(identifier, info);
        // add an image
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        latch.await(10, TimeUnit.SECONDS);

        // assert that they've been added
        Assert.assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // purge everything
        instance.purge();

        // assert that the bucket is empty
        AtomicInteger counter = new AtomicInteger();
        S3Utils.walkObjects(S3Cache.getClientInstance(), instance.getBucket(),
                (object) -> counter.incrementAndGet());
        assertEquals(0, counter.get());
    }

    @Test
    void purgeWithKeyPrefix() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.S3CACHE_OBJECT_KEY_PREFIX.key(), "prefix/");

        Identifier identifier = new Identifier("identifier");
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info = new Info();

        // Add an object outside the prefix
        final S3Client client         = S3Cache.getClientInstance();
        final String bucket           = ConfigurationService.getConfiguration()
                .getString(Key.S3CACHE_BUCKET.key());
        final String keyOutsidePrefix = "some-key";
        final byte[] data             = "some data".getBytes(StandardCharsets.UTF_8);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(keyOutsidePrefix)
                .build();
        try (ByteArrayInputStream is = new ByteArrayInputStream(data)) {
            client.putObject(request,
                    RequestBody.fromInputStream(is, data.length));
        }

        // Cache a variant image (will be written asynchronously so we'll use
        // an observer to wait for it to finish)
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // Cache an info
        instance.put(identifier, info);

        latch.await();

        // assert that everything has been added
        Assert.assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));
        Assertions.assertTrue(S3Utils.objectExists(client, bucket, keyOutsidePrefix));

        // do the purge
        instance.purge();

        // assert that the info has been evicted
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // assert that the image has been evicted
        Assert.assertNotExists(instance, opList);

        // assert that the other file has NOT been evicted
        assertTrue(S3Utils.objectExists(client, bucket, keyOutsidePrefix));
    }

    /* put(Identifier, Info) */

    @Test
    void putWithInfo() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();

        instance.put(identifier, info);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link InfoCache#put(Identifier, Info)}
     * and {@link InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithInfoConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, info);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }).run();
    }

    /* put(Identifier, String) */

    @Test
    void putWithString() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        instance.put(identifier, infoStr);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link InfoCache#put(Identifier, String)}
     * and {@link InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithStringConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, infoStr);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }).run();
    }

}
