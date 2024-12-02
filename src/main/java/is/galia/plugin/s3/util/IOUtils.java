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

package is.galia.plugin.s3.util;

import is.galia.async.VirtualThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public final class IOUtils {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IOUtils.class);

    public static void consumeAndCloseStreamAsync(InputStream inputStream) {
        VirtualThreadPool.getInstance().submit(() -> {
            consumeStream(inputStream);
            try {
                inputStream.close();
            } catch (IOException e) {
                LOGGER.warn("consumeStreamAsync(): failed to close the stream: {}",
                        e.getMessage());
            }
        });
    }

    private static void consumeStream(InputStream inputStream) {
        try {
            inputStream.readAllBytes();
        } catch (IOException e) {
            LOGGER.warn("consumeStream(): failed to consume the stream: {}",
                    e.getMessage());
        }
    }

    private IOUtils() {}

}
