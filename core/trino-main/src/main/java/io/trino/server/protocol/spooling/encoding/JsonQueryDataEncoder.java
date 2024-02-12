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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.JsonArrayResultsIterator;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.client.spooling.DataAttribute.BYTE_SIZE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String ENCODING_ID = "json-ext";
    private final ObjectMapper mapper;
    private final Session session;
    private final List<OutputColumn> columns;

    public JsonQueryDataEncoder(ObjectMapper mapper, Session session, List<OutputColumn> columns)
    {
        this.mapper = requireNonNull(mapper, "mapper is null");
        this.session = requireNonNull(session, "session is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        ImmutableList.Builder<Throwable> serializationExceptions = ImmutableList.builder();
        JsonArrayResultsIterator values = new JsonArrayResultsIterator(
                session,
                pages,
                columns,
                serializationExceptions::add);

        try {
            CountingOutputStream wrapper = new CountingOutputStream(output);
            mapper.writeValue(wrapper, values);
            List<Throwable> exceptions = serializationExceptions.build();
            if (!exceptions.isEmpty()) {
                throw new RuntimeException("Could not serialize to JSON", exceptions.getFirst());
            }

            return DataAttributes.builder()
                    .set(BYTE_SIZE, toIntExact(wrapper.getCount()))
                    .build();
        }
        catch (JsonProcessingException e) {
            throw new IOException("Could not serialize to JSON", e);
        }
    }

    @Override
    public String encodingId()
    {
        return ENCODING_ID;
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        protected final ObjectMapper mapper;

        @Inject
        public Factory(ObjectMapper mapper)
        {
            this.mapper = requireNonNull(mapper, "mapper is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new JsonQueryDataEncoder(mapper, session, columns);
        }

        @Override
        public String encodingId()
        {
            return ENCODING_ID;
        }
    }

    public static class ZstdFactory
            extends Factory
    {
        @Inject
        public ZstdFactory(ObjectMapper mapper)
        {
            super(mapper);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ZstdQueryDataEncoder(super.create(session, columns));
        }

        @Override
        public String encodingId()
        {
            return super.encodingId() + "+zstd";
        }
    }

    public static class SnappyFactory
            extends Factory
    {
        @Inject
        public SnappyFactory(ObjectMapper mapper)
        {
            super(mapper);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new SnappyQueryDataEncoder(super.create(session, columns));
        }

        @Override
        public String encodingId()
        {
            return super.encodingId() + "+snappy";
        }
    }

    public static class Lz4Factory
            extends Factory
    {
        @Inject
        public Lz4Factory(ObjectMapper mapper)
        {
            super(mapper);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new Lz4QueryDataEncoder(super.create(session, columns));
        }

        @Override
        public String encodingId()
        {
            return super.encodingId() + "+lz4";
        }
    }
}
