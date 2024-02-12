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

import io.trino.client.QueryDataDecoder;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Verify.verify;

public class QueryDataDecoders
{
    private static Map<String, QueryDataDecoder.Factory> factories = new HashMap<>();

    private static void register(QueryDataDecoder.Factory factory)
    {
        if (factories.containsKey(factory.encodingId())) {
            throw new IllegalStateException("Encoding " + factory.encodingId() + " already registered.");
        }
        factories.put(factory.encodingId(), factory);
    }

    static
    {
        register(new JsonQueryDataDecoder.Factory());
        register(new JsonQueryDataDecoder.ZstdFactory());
        register(new JsonQueryDataDecoder.SnappyFactory());
        register(new JsonQueryDataDecoder.Lz4Factory());
    }

    private QueryDataDecoders() {}

    public static QueryDataDecoder.Factory get(String encodingId)
    {
        if (!factories.containsKey(encodingId)) {
            throw new IllegalArgumentException("Unknown encoding id: " + encodingId);
        }

        QueryDataDecoder.Factory factory = factories.get(encodingId);
        verify(factory.encodingId().equals(encodingId), "Factory has wrong encoding id, expected %s, got %s", encodingId, factory.encodingId());
        return new DecryptingQueryDataDecoder.Factory(factory);
    }
}
