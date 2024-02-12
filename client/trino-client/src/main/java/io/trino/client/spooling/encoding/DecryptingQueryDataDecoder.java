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
package io.trino.client.spooling.encoding;

import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import jakarta.annotation.Nullable;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static io.trino.client.spooling.DataAttribute.ENCRYPTION_CIPHER_NAME;
import static io.trino.client.spooling.DataAttribute.ENCRYPTION_KEY;
import static io.trino.client.spooling.encoding.CipherUtils.deserializeSecretKey;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;

public class DecryptingQueryDataDecoder
        implements QueryDataDecoder
{
    private final QueryDataDecoder delegate;
    private final SecretKeySpec encryptionKey;

    public DecryptingQueryDataDecoder(QueryDataDecoder delegate, SecretKeySpec encryptionKey)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
    }

    @Override
    public @Nullable Iterable<List<Object>> decode(@Nullable InputStream input, DataAttributes attributes)
            throws IOException
    {
        try (CipherInputStream encryptedInput = new CipherInputStream(input, createCipher(encryptionKey))) {
            return delegate.decode(encryptedInput, attributes);
        }
    }

    @Override
    public String encodingId()
    {
        return delegate.encodingId();
    }

    private static Cipher createCipher(SecretKeySpec key)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(DECRYPT_MODE, key);
            return cipher;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize cipher", e);
        }
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        private final QueryDataDecoder.Factory delegateFactory;

        public Factory(QueryDataDecoder.Factory delegateFactory)
        {
            this.delegateFactory = requireNonNull(delegateFactory, "delegateFactory is null");
        }

        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            QueryDataDecoder delegate = delegateFactory.create(columns, queryAttributes);

            Optional<SecretKeySpec> encryptionKey = queryAttributes.getOptional(ENCRYPTION_KEY, String.class)
                    .map(key -> deserializeSecretKey(key, queryAttributes.get(ENCRYPTION_CIPHER_NAME, String.class)));

            if (encryptionKey.isPresent()) {
                return new DecryptingQueryDataDecoder(delegate, encryptionKey.get());
            }

            return delegate;
        }

        @Override
        public String encodingId()
        {
            return delegateFactory.encodingId();
        }
    }
}
