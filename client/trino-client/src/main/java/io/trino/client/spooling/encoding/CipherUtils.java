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

import javax.crypto.spec.SecretKeySpec;

import static java.util.Base64.getMimeDecoder;
import static java.util.Base64.getMimeEncoder;

public class CipherUtils
{
    private CipherUtils() {}

    public static String serializeSecretKey(SecretKeySpec key)
    {
        return getMimeEncoder().encodeToString(key.getEncoded());
    }

    public static SecretKeySpec deserializeSecretKey(String serializedKey, String cipherName)
    {
        return new SecretKeySpec(getMimeDecoder().decode(serializedKey), cipherName);
    }
}
