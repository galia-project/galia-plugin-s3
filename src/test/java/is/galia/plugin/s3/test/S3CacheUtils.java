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
import is.galia.plugin.s3.cache.S3Cache;
import is.galia.plugin.s3.config.Key;

public final class S3CacheUtils {

    public static void createBucket() {
        S3TestUtils.createBucket(
                S3Cache.getClientInstance(),
                getBucket());
    }

    public static void deleteBucket() {
        S3TestUtils.deleteBucket(
                S3Cache.getClientInstance(),
                getBucket());
    }

    public static void emptyBucket() {
        S3TestUtils.emptyBucket(
                S3Cache.getClientInstance(),
                getBucket());
    }

    private static String getBucket() {
        Configuration config = ConfigurationService.getConfiguration();
        return config.getString(Key.S3CACHE_BUCKET.key());
    }

    private S3CacheUtils() {}

}
