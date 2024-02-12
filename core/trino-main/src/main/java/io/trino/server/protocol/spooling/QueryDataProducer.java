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

import io.trino.Session;
import io.trino.client.QueryData;
import io.trino.client.RawQueryData;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.QueryResultRows;
import io.trino.spi.Page;
import jakarta.ws.rs.core.UriBuilder;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.server.protocol.JsonArrayResultsIterator.toIterableList;
import static io.trino.server.protocol.spooling.SegmentResource.spooledSegmentUriBuilder;
import static java.util.Objects.requireNonNull;

public class QueryDataProducer
{
    private final Optional<QueryDataEncoder.Factory> encoderFactory;
    private long currentOffset;

    private AtomicBoolean metadataWritten = new AtomicBoolean(false);

    public QueryDataProducer(Optional<QueryDataEncoder.Factory> encoderFactory)
    {
        this.encoderFactory = requireNonNull(encoderFactory, "encoderFactory is null");
    }

    public QueryData produce(ExternalUriInfo uriInfo, Session session, QueryResultRows rows, Consumer<Throwable> throwableConsumer)
    {
        if (rows.isEmpty()) {
            return null;
        }

        if (encoderFactory.isEmpty()) {
            return RawQueryData.of(toIterableList(session, rows, throwableConsumer));
        }

        UriBuilder uriBuilder = spooledSegmentUriBuilder(uriInfo);
        QueryDataEncoder encoder = encoderFactory.get().create(session, rows.getOutputColumns().orElseThrow());
        EncodedQueryData.Builder builder = EncodedQueryData.builder(encoder.encodingId());
        List<OutputColumn> outputColumns = rows.getOutputColumns().orElseThrow();

        if (metadataWritten.compareAndSet(false, true)) {
            // Attributes are emitted only once for the first segment
            builder.withAttributes(encoder.attributes());
        }

        try {
            for (Page page : rows.getPages()) {
                if (hasSpoolingMetadata(page, outputColumns)) {
                    SpooledBlock metadata = SpooledBlock.deserialize(page);
                    DataAttributes attributes = metadata.attributes().toBuilder()
                            .set(ROW_OFFSET, currentOffset)
                            .build();

                    builder.withSegment(Segment.spooled(buildSegmentURI(uriBuilder, metadata.segmentHandle()), attributes));
                    currentOffset += attributes.get(ROWS_COUNT, Long.class);
                }
                else {
                    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                        DataAttributes attributes = encoder.encodeTo(output, List.of(page))
                                .toBuilder()
                                .set(ROW_OFFSET, currentOffset)
                                .build();
                        builder.withSegment(Segment.inlined(output.toByteArray(), attributes));
                    }
                    currentOffset += page.getPositionCount();
                }
            }
        }
        catch (Exception e) {
            throwableConsumer.accept(e);
            return null;
        }

        return builder.build();
    }

    private URI buildSegmentURI(UriBuilder builder, String segmentHandle)
    {
        return builder.clone().build(segmentHandle);
    }

    private boolean hasSpoolingMetadata(Page page, List<OutputColumn> outputColumns)
    {
        return page.getChannelCount() == outputColumns.size() + 1 && page.getPositionCount() == 1 && !page.getBlock(outputColumns.size()).isNull(0);
    }
}
