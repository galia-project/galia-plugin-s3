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

package is.galia.plugin.s3;

import is.galia.config.Configuration;
import is.galia.config.ConfigurationFactory;
import is.galia.plugin.s3.test.ConfigurationService;
import is.galia.plugin.s3.test.S3SourceUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;

public abstract class BaseTest {

    protected static final Path FIXTURE = S3SourceUtils.getFixture("ghost.png");

    @BeforeAll
    static void beforeEverything() {
        reloadConfiguration();
    }

    private static void reloadConfiguration() {
        Configuration config = ConfigurationService.getConfiguration();
        ConfigurationFactory.setAppInstance(config);
    }

    @BeforeEach
    public void setUp() throws Exception {
        reloadConfiguration();
    }

    @AfterEach
    public void tearDown() {
    }

}
