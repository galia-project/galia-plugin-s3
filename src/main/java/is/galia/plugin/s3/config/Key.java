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

package is.galia.plugin.s3.config;

public enum Key {

    S3CACHE_ACCESS_KEY_ID          ("cache.S3Cache.access_key_id"),
    S3CACHE_ASYNC_CREDENTIAL_UPDATE("cache.S3Cache.async_credential_update"),
    S3CACHE_BUCKET                 ("cache.S3Cache.bucket"),
    S3CACHE_ENDPOINT               ("cache.S3Cache.endpoint"),
    S3CACHE_MULTIPART_UPLOADS      ("cache.S3Cache.multipart_uploads"),
    S3CACHE_OBJECT_KEY_PREFIX      ("cache.S3Cache.object_key_prefix"),
    S3CACHE_REGION                 ("cache.S3Cache.region"),
    S3CACHE_MAX_RETRIES            ("cache.S3Cache.max_retries"),
    S3CACHE_SECRET_ACCESS_KEY      ("cache.S3Cache.secret_access_key"),

    S3SOURCE_ACCESS_KEY_ID          ("source.S3Source.access_key_id"),
    S3SOURCE_ASYNC_CREDENTIAL_UPDATE("source.S3Source.async_credential_update"),
    S3SOURCE_BUCKET                 ("source.S3Source.BasicLookupStrategy.bucket"),
    S3SOURCE_CHUNKING_ENABLED       ("source.S3Source.chunking.enabled"),
    S3SOURCE_CHUNK_SIZE             ("source.S3Source.chunking.chunk_size"),
    S3SOURCE_ENDPOINT               ("source.S3Source.endpoint"),
    S3SOURCE_LOOKUP_STRATEGY        ("source.S3Source.lookup_strategy"),
    S3SOURCE_PATH_PREFIX            ("source.S3Source.BasicLookupStrategy.path_prefix"),
    S3SOURCE_PATH_SUFFIX            ("source.S3Source.BasicLookupStrategy.path_suffix"),
    S3SOURCE_REGION                 ("source.S3Source.region"),
    S3SOURCE_SECRET_ACCESS_KEY      ("source.S3Source.secret_access_key");

    private final String key;

    Key(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public String toString() {
        return key();
    }

}
