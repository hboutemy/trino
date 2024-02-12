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
package io.trino.server.protocol.spooling.encoding;

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.spooling.DataAttribute.ENCRYPTION_CIPHER_NAME;
import static io.trino.client.spooling.DataAttribute.ENCRYPTION_KEY;
import static io.trino.client.spooling.encoding.CipherUtils.serializeSecretKey;
import static io.trino.util.Ciphers.deserializeAesEncryptionKey;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class EncryptingQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String CIPHER_NAME = "AES";
    private final QueryDataEncoder delegate;
    private final SecretKeySpec key;

    public EncryptingQueryDataEncoder(QueryDataEncoder delegate, Slice encryptionKey)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.key = deserializeAesEncryptionKey(requireNonNull(encryptionKey, "encryptionKey is null"));
        checkArgument(is256BitSecretKeySpec(key), "encryptionKey is expected to be an instance of SecretKeySpec containing a 256bit AES key");
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        try (CipherOutputStream encryptedOutput = new CipherOutputStream(output, createCipher(key))) {
            return delegate.encodeTo(encryptedOutput, pages);
        }
    }

    @Override
    public DataAttributes attributes()
    {
        return delegate
                .attributes()
                .toBuilder()
                .set(ENCRYPTION_KEY, serializeSecretKey(key))
                .set(ENCRYPTION_CIPHER_NAME, CIPHER_NAME)
                .build();
    }

    @Override
    public String encodingId()
    {
        return delegate.encodingId();
    }

    private static Cipher createCipher(SecretKeySpec privateKey)
    {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_NAME);
            cipher.init(ENCRYPT_MODE, privateKey);
            return cipher;
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        private final QueryDataEncoder.Factory delegate;

        public Factory(QueryDataEncoder.Factory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            QueryDataEncoder encoder = delegate.create(session, columns);
            if (session.getQueryDataEncryptionKey().isEmpty()) {
                return encoder;
            }
            return new EncryptingQueryDataEncoder(encoder, session.getQueryDataEncryptionKey().orElseThrow());
        }

        @Override
        public String encodingId()
        {
            return delegate.encodingId();
        }
    }
}
