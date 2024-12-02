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

import is.galia.plugin.s3.config.Key;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.util.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import is.galia.config.Configuration;

public final class S3SourceClientService {

    private static S3Client client;

    public static synchronized S3Client getClient() {
        if (client == null) {
            String endpoint        = getEndpoint();
            String region          = getRegion();
            String accessKeyID     = getAccessKeyID();
            String secretAccessKey = getSecretAccessKey();
            client = new S3ClientBuilder()
                    .endpoint(endpoint)
                    .region(region)
                    .accessKeyID(accessKeyID)
                    .secretAccessKey(secretAccessKey)
                    .build();
        }
        return client;
    }

    private static String getAccessKeyID() {
        Configuration testConfig = ConfigurationService.getConfiguration();
        return testConfig.getString(Key.S3SOURCE_ACCESS_KEY_ID.key());
    }

    private static String getEndpoint() {
        Configuration testConfig = ConfigurationService.getConfiguration();
        return testConfig.getString(Key.S3SOURCE_ENDPOINT.key());
    }

    private static String getRegion() {
        Configuration testConfig = ConfigurationService.getConfiguration();
        return testConfig.getString(Key.S3SOURCE_REGION.key());
    }

    private static String getSecretAccessKey() {
        Configuration testConfig = ConfigurationService.getConfiguration();
        return testConfig.getString(Key.S3SOURCE_SECRET_ACCESS_KEY.key());
    }

    private S3SourceClientService() {}

}
