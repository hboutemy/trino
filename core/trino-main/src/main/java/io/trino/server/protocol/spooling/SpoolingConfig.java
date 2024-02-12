/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.protocol.spooling;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.AssertTrue;

import javax.crypto.spec.SecretKeySpec;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.util.Base64.getDecoder;

public class SpoolingConfig
{
    private boolean enabled;
    private boolean useWorkers;
    private boolean directStorageAccess;

    private boolean inlineSegments = true;
    private boolean encryptionEnabled = true;

    private DataSize initialSegmentSize = DataSize.of(8, MEGABYTE);
    private DataSize maximumSegmentSize = DataSize.of(16, MEGABYTE);

    private SecretKeySpec encryptionKey;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("protocol.spooling.enabled")
    @ConfigDescription("Enable spooling client protocol server-side support")
    public SpoolingConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public boolean isUseWorkers()
    {
        return useWorkers;
    }

    @Config("protocol.spooling.worker-access")
    @ConfigDescription("Use worker nodes to retrieve data from spooling location")
    public SpoolingConfig setUseWorkers(boolean useWorkers)
    {
        this.useWorkers = useWorkers;
        return this;
    }

    public boolean isDirectStorageAccess()
    {
        return directStorageAccess;
    }

    @Config("protocol.spooling.direct-storage-access")
    @ConfigDescription("Allow clients to directly access spooled segments (if supported by spooling manager)")
    public SpoolingConfig setDirectStorageAccess(boolean directStorageAccess)
    {
        this.directStorageAccess = directStorageAccess;
        return this;
    }

    public DataSize getInitialSegmentSize()
    {
        return initialSegmentSize;
    }

    @Config("protocol.spooling.initial-segment-size")
    @ConfigDescription("Initial size of the spooled segments in bytes")
    public void setInitialSegmentSize(DataSize initialSegmentSize)
    {
        this.initialSegmentSize = initialSegmentSize;
    }

    public DataSize getMaximumSegmentSize()
    {
        return maximumSegmentSize;
    }

    @Config("protocol.spooling.maximum-segment-size")
    @ConfigDescription("Maximum size of the spooled segments in bytes")
    public void setMaximumSegmentSize(DataSize maximumSegmentSize)
    {
        this.maximumSegmentSize = maximumSegmentSize;
    }

    public boolean isInlineSegments()
    {
        return inlineSegments;
    }

    @ConfigDescription("Allow protocol to inline data")
    @Config("protocol.spooling.use-inline-segments")
    public void setInlineSegments(boolean inlineSegments)
    {
        this.inlineSegments = inlineSegments;
    }

    public boolean isEncryptionEnabled()
    {
        return encryptionEnabled;
    }

    @ConfigDescription("Encrypt spooled segments using random, ephemeral keys generated for the duration of the query")
    @Config("protocol.spooling.encryption")
    public void setEncryptionEnabled(boolean encryptionEnabled)
    {
        this.encryptionEnabled = encryptionEnabled;
    }

    public SecretKeySpec getEncryptionKey()
    {
        return encryptionKey;
    }

    @ConfigDescription("256 bit, base64-encoded secret key used to secure segment identifiers")
    @Config("protocol.spooling.encryption-key")
    @ConfigSecuritySensitive
    public void setEncryptionKey(String encryptionKey)
    {
        this.encryptionKey = new SecretKeySpec(getDecoder().decode(encryptionKey), "AES");
    }

    @AssertTrue(message = "protocol.spooling.encryption-key must be 256 bits long")
    public boolean isEncryptionKeyAes256()
    {
        return encryptionKey == null || is256BitSecretKeySpec(encryptionKey);
    }

    @AssertTrue(message = "protocol.spooling.encryption-key must be set if spooling is enabled")
    public boolean isEncryptionKeySet()
    {
        if (!enabled) {
            return true;
        }
        return encryptionKey != null;
    }
}
