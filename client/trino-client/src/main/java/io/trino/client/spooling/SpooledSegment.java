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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SpooledSegment
        extends Segment
{
    private final URI dataUri;

    @JsonCreator
    public SpooledSegment(@JsonProperty("dataUri") URI dataUri, @JsonProperty("metadata") Map<String, Object> metadata)
    {
        this(dataUri, new DataAttributes(metadata));
    }

    SpooledSegment(URI dataUri, DataAttributes metadata)
    {
        super(metadata);
        this.dataUri = requireNonNull(dataUri, "dataUri is null");
    }

    @JsonProperty("dataUri")
    public URI getDataUri()
    {
        return dataUri;
    }

    @Override
    public String toString()
    {
        return format("SpooledSegment{offset=%d, rows=%d, size=%d}", getOffset(), getRowsCount(), getDataSizeBytes());
    }
}
