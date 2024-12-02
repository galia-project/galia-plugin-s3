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

import is.galia.config.Configuration;
import is.galia.delegate.Delegate;
import is.galia.delegate.DelegateException;
import is.galia.plugin.s3.source.TestDelegate;
import is.galia.resource.RequestContext;
import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.source.S3SourceClientService;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class S3SourceUtils {

    public static final String OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION      = "ghost1.png";
    public static final String OBJECT_KEY_WITH_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION    = "ghost1.unknown";
    public static final String OBJECT_KEY_WITH_CONTENT_TYPE_BUT_NO_EXTENSION              = "ghost1";
    public static final String OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION   = "ghost2.png";
    public static final String OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION = "ghost2.unknown";
    public static final String OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION    = "ghost2.jpg";
    public static final String OBJECT_KEY_WITH_NO_CONTENT_TYPE_OR_EXTENSION               = "ghost2";
    public static final String NON_IMAGE_OBJECT_KEY                                       = "NotAnImage";

    public static void createBucket() {
        final Configuration config = ConfigurationService.getConfiguration();
        S3TestUtils.createBucket(
                S3SourceClientService.getClient(),
                config.getString(Key.S3SOURCE_BUCKET.key()));
    }

    public static void deleteBucket() {
        final Configuration config = ConfigurationService.getConfiguration();
        S3TestUtils.deleteBucket(
                S3SourceClientService.getClient(),
                config.getString(Key.S3SOURCE_BUCKET.key()));
    }

    public static void deleteFixtures() {
        final Configuration config = ConfigurationService.getConfiguration();
        final String bucket = config.getString(Key.S3SOURCE_BUCKET.key());
        S3TestUtils.emptyBucket(S3SourceClientService.getClient(), bucket);
    }

    public static Path getFixture(String filename) {
        return getFixturePath().resolve(filename);
    }

    /**
     * @return Path of the fixtures directory.
     */
    static Path getFixturePath() {
        return Paths.get("src", "test", "resources");
    }

    public static Delegate newDelegate() {
        try {
            Delegate delegate = new TestDelegate();
            delegate.setRequestContext(new RequestContext());
            return delegate;
        } catch (DelegateException e) {
            throw new RuntimeException(e);
        }
    }

    public static void seedFixtures() {
        final S3Client client = S3SourceClientService.getClient();
        final String bucket = ConfigurationService.getConfiguration()
                .getString(Key.S3SOURCE_BUCKET.key());
        Path fixture = S3SourceUtils.getFixture("ghost.png");

        for (final String key : new String[] {
                OBJECT_KEY_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION,
                OBJECT_KEY_WITH_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION,
                OBJECT_KEY_WITH_CONTENT_TYPE_BUT_NO_EXTENSION,
                OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION,
                OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION,
                OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION,
                OBJECT_KEY_WITH_NO_CONTENT_TYPE_OR_EXTENSION}) {
            String contentType = null;
            if (!OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION.equals(key) &&
                    !OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION.equals(key) &&
                    !OBJECT_KEY_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION.equals(key) &&
                    !OBJECT_KEY_WITH_NO_CONTENT_TYPE_OR_EXTENSION.equals(key)) {
                contentType = "image/png";
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(contentType)
                    .build();
            client.putObject(request, fixture);
        }

        // Add a non-image
        fixture = S3SourceUtils.getFixture("non_image.txt");
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(NON_IMAGE_OBJECT_KEY)
                .build();
        client.putObject(request, fixture);
    }

    private S3SourceUtils() {}


}
