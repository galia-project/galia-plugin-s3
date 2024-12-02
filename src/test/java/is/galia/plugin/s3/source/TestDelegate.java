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

import is.galia.config.Configuration;
import is.galia.delegate.Delegate;
import is.galia.plugin.Plugin;
import is.galia.plugin.s3.config.Key;
import is.galia.resource.RequestContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestDelegate implements Delegate, Plugin {

    private RequestContext requestContext;

    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Set.of();
    }

    @Override
    public String getPluginName() {
        return TestDelegate.class.getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void initializePlugin() {}

    @Override
    public void onApplicationStop() {}

    //endregion
    //region Delegate methods

    @Override
    public void setRequestContext(RequestContext context) {
        this.requestContext = context;
    }

    @Override
    public RequestContext getRequestContext() {
        return requestContext;
    }

    @SuppressWarnings("unused")
    public Object s3source_object_info() {
        String identifier = getRequestContext().getIdentifier().toString();
        if (identifier.contains("bucket:") || identifier.contains("key:")) {
            Map<String,String> map = new HashMap<>();
            String[] parts = identifier.split(";");
            for (String part : parts) {
                String[] kv = part.split(":");
                map.put(kv[0], kv[1]);
            }
            return map;
        } else if (identifier.equals("bogus")) {
            return null;
        }
        Configuration config = Configuration.forApplication();
        String bucketName = config.getString(Key.S3SOURCE_BUCKET.key());
        return Map.of("bucket", bucketName, "key", identifier);
    }

}
