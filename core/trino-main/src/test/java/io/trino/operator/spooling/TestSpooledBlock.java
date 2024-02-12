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
package io.trino.operator.spooling;

import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.spooling.SpooledBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.client.spooling.DataAttribute.BYTE_SIZE;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class TestSpooledBlock
{
    @Test
    public void testSerializationRoundTrip()
    {
        SpooledBlock metadata = new SpooledBlock("segmentId", createDataAttributes(10, 1200));
        Page page = new Page(metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    @Test
    public void testSerializationRoundTripWithNonEmptyPage()
    {
        SpooledBlock metadata = new SpooledBlock("segmentId", createDataAttributes(10, 1100));
        Page page = new Page(blockWithPositions(1, true), metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    @Test
    public void testThrowsErrorOnNonNullPositions()
    {
        SpooledBlock metadata = new SpooledBlock("segmentId", createDataAttributes(20, 1200));

        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(1, false), metadata.serialize())))
                .hasMessage("Spooling metadata block must have all but last channels null");
    }

    @Test
    public void testThrowsErrorOnMultiplePositions()
    {
        SpooledBlock metadata = new SpooledBlock("segmentId", createDataAttributes(30, 1300));

        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(2, false), metadata.serialize())))
                .hasMessage("Spooling metadata block must have a single position");
    }

    public static Block blockWithPositions(int count, boolean isNull)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, count);
        for (int i = 0; i < count; i++) {
            if (isNull) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, 0);
            }
        }
        return blockBuilder.build();
    }

    private static DataAttributes createDataAttributes(long rows, int dataSizeBytes)
    {
        return DataAttributes.builder()
                .set(ROWS_COUNT, rows)
                .set(BYTE_SIZE, dataSizeBytes)
                .build();
    }
}
