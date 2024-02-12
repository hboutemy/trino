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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Base64.getUrlDecoder;
import static java.util.Base64.getUrlEncoder;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class SpoolingManagerBridge
{
    private final SpoolingManagerRegistry registry;
    private final DataSize initialSegmentSize;
    private final DataSize maximumSegmentSize;
    private final boolean inlineSegments;
    private final SecretKeySpec secretKey;

    @Inject
    public SpoolingManagerBridge(SpoolingConfig spoolingConfig, SpoolingManagerRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
        requireNonNull(spoolingConfig, "spoolingConfig is null");
        this.initialSegmentSize = spoolingConfig.getInitialSegmentSize();
        this.maximumSegmentSize = spoolingConfig.getMaximumSegmentSize();
        this.inlineSegments = spoolingConfig.isInlineSegments();
        this.secretKey = spoolingConfig.getEncryptionKey();
    }

    public boolean isLoaded()
    {
        return registry
                .getSpoolingManager()
                .isPresent();
    }

    public long maximumSegmentSize()
    {
        return maximumSegmentSize.toBytes();
    }

    public long initialSegmentSize()
    {
        return initialSegmentSize.toBytes();
    }

    public boolean useInlineSegments()
    {
        return inlineSegments;
    }

    public SpooledSegmentHandle create(SpoolingContext context)
    {
        return delegate().create(context);
    }

    public OutputStream createOutputStream(Object handle)
            throws Exception
    {
        return delegate().createOutputStream(decodeHandle(handle));
    }

    public Optional<URI> directLocation(Object handle)
    {
        return delegate().directLocation(decodeHandle(handle));
    }

    public InputStream openInputStream(Object handle)
            throws IOException
    {
        return delegate().openInputStream(decodeHandle(handle));
    }

    public void drop(Object segmentId)
            throws IOException
    {
        delegate().acknowledge(decodeHandle(segmentId));
    }

    private SpooledSegmentHandle decodeHandle(Object handle)
    {
        if (handle instanceof SpooledSegmentHandle spooledHandle) {
            return spooledHandle;
        }

        if (handle instanceof String stringValue) {
            return delegate().deserialize(decrypt(wrappedBuffer(getUrlDecoder().decode(stringValue))));
        }

        throw new IllegalArgumentException("Unsupported segment id format: " + handle.getClass().getSimpleName());
    }

    public String handleToUriIdentifier(SpooledSegmentHandle handle)
    {
        return getUrlEncoder().encodeToString(encrypt(delegate().serialize(handle)).getBytes());
    }

    private SpoolingManager delegate()
    {
        return registry
                .getSpoolingManager()
                .orElseThrow(() -> new IllegalStateException("Spooling manager is not loaded"));
    }

    private Slice encrypt(Slice input)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(ENCRYPT_MODE, secretKey);
            return wrappedBuffer(cipher.doFinal(input.getBytes()));
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException |
               BadPaddingException e) {
            throw new RuntimeException("Could not encrypt segment handle", e);
        }
    }

    private Slice decrypt(Slice input)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(DECRYPT_MODE, secretKey);
            return wrappedBuffer(cipher.doFinal(input.getBytes()));
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException |
               BadPaddingException e) {
            throw new RuntimeException("Could not decrypt segment handle", e);
        }
    }
}
