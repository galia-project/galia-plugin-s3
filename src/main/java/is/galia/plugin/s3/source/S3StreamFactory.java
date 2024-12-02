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

import is.galia.async.VirtualThreadPool;
import is.galia.config.Configuration;
import is.galia.plugin.s3.config.Key;
import is.galia.stream.ClosingFileCacheImageInputStream;
import is.galia.stream.HTTPImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import is.galia.util.IOUtils;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Source of streams for {@link S3Source}.
 */
final class S3StreamFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(S3StreamFactory.class);

    private static final int DEFAULT_CHUNK_SIZE       = 1024 * 512;
    private static final int DEFAULT_CHUNK_CACHE_SIZE = 1024 * 1024 * 10;

    private final S3ObjectInfo objectInfo;

    S3StreamFactory(S3ObjectInfo objectInfo) {
        this.objectInfo = objectInfo;
    }

    ImageInputStream newSeekableStream() throws IOException {
        if (isChunkingEnabled()) {
            final int chunkSize = getChunkSize();
            LOGGER.debug("newSeekableStream(): using {}-byte chunks",
                    chunkSize);

            final S3HTTPImageInputStreamClient client =
                    new S3HTTPImageInputStreamClient(objectInfo);

            HTTPImageInputStream stream = new HTTPImageInputStream(
                    client, objectInfo.getLength());
            try {
                stream.setWindowSize(chunkSize);
                return stream;
            } catch (Throwable t) {
                IOUtils.closeQuietly(stream);
                throw t;
            }
        } else {
            LOGGER.debug("newSeekableStream(): chunking is disabled");
            return new ClosingFileCacheImageInputStream(newInputStream());
        }
    }

    private InputStream newInputStream() throws IOException {
        final InputStream responseStream =
                S3Source.newObjectInputStream(objectInfo);

        // Ideally we would just return responseStream. However, if it is
        // close()d before being fully consumed, its underlying TCP connection
        // will also be closed, negating the advantage of the connection pool
        // and triggering a warning log message from the S3 client.
        //
        // This wrapper stream's close() method will drain the stream
        // before closing it. Because this may take a long time, it will be
        // done in another thread.
        return new InputStream() {
            private boolean isClosed;

            @Override
            public void close() {
                if (!isClosed) {
                    isClosed = true;
                    VirtualThreadPool.getInstance().submit(() -> {
                        try {
                            try {
                                while (responseStream.read() != -1) {
                                    // drain the stream
                                }
                            } finally {
                                try {
                                    responseStream.close();
                                } finally {
                                    super.close();
                                }
                            }
                        } catch (IOException e) {
                            LOGGER.warn(e.getMessage(), e);
                        }
                    });
                }
            }

            @Override
            public int read() throws IOException {
                return responseStream.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return responseStream.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return responseStream.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                return responseStream.skip(n);
            }
        };
    }

    private boolean isChunkingEnabled() {
        return Configuration.forApplication().getBoolean(
                Key.S3SOURCE_CHUNKING_ENABLED.key(), true);
    }

    private int getChunkSize() {
        return (int) Configuration.forApplication().getLongBytes(
                Key.S3SOURCE_CHUNK_SIZE.key(), DEFAULT_CHUNK_SIZE);
    }

}
