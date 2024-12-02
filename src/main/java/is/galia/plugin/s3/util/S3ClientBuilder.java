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

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider;
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain;
import software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider;
import software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Creates an S3 client using the Builder pattern.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/welcome.html">
 *     AWS SDK for Java</a>
 */
public final class S3ClientBuilder {

    /**
     * This region is used when the region provider chain used by {@link
     * #getEffectiveRegion()} is not able to obtain a region.
     */
    private static final Region DEFAULT_REGION = Region.US_EAST_1;

    private URI endpointURI;
    private Region region;
    private String accessKeyID, secretAccessKey;
    private boolean isCredentialUpdateAsync = true;

    /**
     * @param accessKeyID AWS access key ID.
     * @return            The instance.
     */
    public S3ClientBuilder accessKeyID(String accessKeyID) {
        this.accessKeyID = accessKeyID;
        return this;
    }

    public S3ClientBuilder asyncCredentialUpdateEnabled(boolean asyncCredentialUpdateEnabled) {
        this.isCredentialUpdateAsync = asyncCredentialUpdateEnabled;
        return this;
    }

    /**
     * @param uri URI of the S3 endpoint. If not supplied, an AWS endpoint is
     *            used based on {@link #region(String)}.
     * @return    The instance.
     */
    public S3ClientBuilder endpoint(String uri) {
        if (uri != null && !uri.isBlank()) {
            endpointURI(URI.create(uri));
        }
        return this;
    }

    /**
     * @param uri URI of the S3 endpoint. If not supplied, an AWS endpoint is
     *            used based on {@link #region(String)}.
     * @return    The instance.
     */
    public S3ClientBuilder endpointURI(URI uri) {
        this.endpointURI = uri;
        return this;
    }

    /**
     * @param region Region to use. This is relevant only for AWS endpoints.
     * @return       The instance.
     */
    public S3ClientBuilder region(String region) {
        try {
            this.region = (region != null) ? Region.of(region) : null;
        } catch (IllegalArgumentException | SdkClientException e) {
            this.region = null;
        }
        return this;
    }

    /**
     * @param secretAccessKey AWS secret access key.
     * @return          The instance.
     */
    public S3ClientBuilder secretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public S3Client build() {
        final S3Configuration config = S3Configuration.builder()
                .pathStyleAccessEnabled(endpointURI != null)
                .checksumValidationEnabled(false)
                .build();
        software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder()
                //.httpClientBuilder(UrlConnectionHttpClient.builder())
                .httpClientBuilder(AwsCrtHttpClient.builder())
                .serviceConfiguration(config)
                // A region is required even for non-AWS endpoints.
                .region(getEffectiveRegion())
                .credentialsProvider(newCredentialsProvider());
        if (endpointURI != null) {
            builder = builder.endpointOverride(endpointURI);
        }
        return builder.build();
    }

    /**
     * Returns a region using a similar strategy as the {@link
     * software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain}
     * except the application configuration is consulted between the
     * environment and AWS profile.
     *
     * @return Region, or {@link #DEFAULT_REGION} if none could be found.
     * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/providers/DefaultAwsRegionProviderChain.html">
     *     DefaultAwsRegionProviderChain</a>
     */
    private Region getEffectiveRegion() {
        try {
            return new AwsRegionProviderChain(
                    new SystemSettingsRegionProvider(),
                    () -> region,
                    new AwsProfileRegionProvider(),
                    new InstanceProfileRegionProvider()).getRegion();
        } catch (SdkClientException e) {
            return DEFAULT_REGION;
        }
    }

    /**
     * Returns credentials using a similar strategy as the {@link
     * software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider}
     * except the application configuration is consulted after the
     * environment.
     *
     * @see <a href="https://sdk.amazonaws.com/java/api/latest/index.html?software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">
     *     AwsCredentialsProvider</a>
     */
    private AwsCredentialsProvider newCredentialsProvider() {
        final AwsCredentialsProviderChain.Builder builder =
                AwsCredentialsProviderChain.builder();
        // SystemPropertyCredentialsProvider
        builder.addCredentialsProvider(SystemPropertyCredentialsProvider.create());
        // EnvironmentVariableCredentialsProvider
        builder.addCredentialsProvider(EnvironmentVariableCredentialsProvider.create());
        // StaticCredentialsProvider
        if (accessKeyID != null && !accessKeyID.isBlank() &&
                secretAccessKey != null && !secretAccessKey.isBlank()) {
            builder.addCredentialsProvider(StaticCredentialsProvider.create(new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return accessKeyID;
                }
                @Override
                public String secretAccessKey() {
                    return secretAccessKey;
                }
            }));
        }
        // WebIdentityTokenFileCredentialsProvider
        builder.addCredentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                .asyncCredentialUpdateEnabled(isCredentialUpdateAsync).build());
        // ProfileCredentialsProvider
        builder.addCredentialsProvider(ProfileCredentialsProvider.create());
        // ContainerCredentialsProvider
        builder.addCredentialsProvider(ContainerCredentialsProvider.builder()
                .asyncCredentialUpdateEnabled(isCredentialUpdateAsync).build());
        // InstanceProfileCredentialsProvider
        builder.addCredentialsProvider(InstanceProfileCredentialsProvider.builder()
                .asyncCredentialUpdateEnabled(isCredentialUpdateAsync).build());
        return builder.build();
    }

}
