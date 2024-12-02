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

import is.galia.http.Reference;
import is.galia.plugin.s3.BaseTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class S3ObjectInfoTest extends BaseTest {

    @Test
    void getReferenceWithoutEndpoint() {
        String bucketName = "mybucket";
        String key        = "mykey";
        S3ObjectInfo info = new S3ObjectInfo(bucketName, key);
        assertEquals(new Reference("s3://" + bucketName + "/" + key),
                info.getReference());
    }

    @Test
    void getReferenceWithEndpoint() {
        String endpoint   = "http://localhost:5000";
        String bucketName = "mybucket";
        String key        = "mykey";
        S3ObjectInfo info = new S3ObjectInfo(
                "us-east-1", endpoint, null, null, bucketName, key);
        assertEquals(
                new Reference("s3://" + endpoint + "/" + bucketName + "/" + key),
                info.getReference());
    }

}
