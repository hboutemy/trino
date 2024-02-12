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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.trino.client.spooling.DataAttributes;

public class DataAttributesSerialization
{
    private static final Joiner.MapJoiner JOINER = Joiner.on(",").withKeyValueSeparator(":");
    private static final Splitter.MapSplitter SPLITTER = Splitter.on(",")
            .trimResults()
            .omitEmptyStrings()
            .withKeyValueSeparator(":");

    private DataAttributesSerialization() {}

    public static String serialize(DataAttributes attributes)
    {
        return JOINER.join(attributes.toMap());
    }

    public static DataAttributes deserialize(String values)
    {
        DataAttributes.Builder builder = DataAttributes.builder();
        SPLITTER.split(values).forEach(builder::set);
        return builder.build();
    }
}
