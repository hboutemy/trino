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

import io.trino.client.spooling.DataAttributes;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

public record SpooledBlock(String segmentHandle, DataAttributes attributes)
{
    public static final RowType SPOOLING_METADATA_TYPE = RowType.from(List.of(
            new RowType.Field(Optional.of("identifier"), VARCHAR),
            new RowType.Field(Optional.of("metadata"), VARCHAR)));

    public static final String SPOOLING_METADATA_COLUMN_NAME = "$spooling:metadata$";
    public static final Symbol SPOOLING_METADATA_SYMBOL = new Symbol(SPOOLING_METADATA_TYPE, SPOOLING_METADATA_COLUMN_NAME);

    public static SpooledBlock deserialize(Page page)
    {
        verify(page.getPositionCount() == 1, "Spooling metadata block must have a single position");
        verify(hasMetadataBlock(page), "Spooling metadata block must have all but last channels null");
        SqlRow row = SPOOLING_METADATA_TYPE.getObject(page.getBlock(page.getChannelCount() - 1), 0);

        return new SpooledBlock(
                VARCHAR.getSlice(row.getRawFieldBlock(0), 0).toStringUtf8(),
                DataAttributesSerialization.deserialize(VARCHAR.getSlice(row.getRawFieldBlock(1), 0).toStringUtf8()));
    }

    public Block serialize()
    {
        RowBlockBuilder rowBlockBuilder = SPOOLING_METADATA_TYPE.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(rowEntryBuilder -> {
            VARCHAR.writeSlice(rowEntryBuilder.get(0), utf8Slice(segmentHandle));
            VARCHAR.writeSlice(rowEntryBuilder.get(1), utf8Slice(DataAttributesSerialization.serialize(attributes)));
        });
        return rowBlockBuilder.build();
    }

    public static Page createNonSpooledPage(Page page)
    {
        RowBlockBuilder rowBlockBuilder = SPOOLING_METADATA_TYPE.createBlockBuilder(null, page.getPositionCount());
        for (int i = 0; i < page.getPositionCount(); i++) {
            rowBlockBuilder.appendNull();
        }
        return page.appendColumn(rowBlockBuilder.build());
    }

    private static boolean hasMetadataBlock(Page page)
    {
        for (int channel = 0; channel < page.getChannelCount() - 1; channel++) {
            if (!page.getBlock(channel).isNull(0)) {
                return false;
            }
        }
        return true;
    }
}
