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
package io.trino.spooling.filesystem;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Date;

import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class FileSystemSpoolingManager
        implements SpoolingManager
{
    private static final Logger log = Logger.get(FileSystemSpoolingManager.class);

    private final String location;
    private final TrinoFileSystem fileSystem;
    private final Duration ttl;

    @Inject
    public FileSystemSpoolingManager(FileSystemSpoolingConfig config, TrinoFileSystemFactory fileSystemFactory)
    {
        requireNonNull(config, "config is null");
        this.location = config.getLocation();
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
                .create(ConnectorIdentity.ofUser("ignored"));
        this.ttl = config.getTtl();
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws Exception
    {
        return fileSystem.newOutputFile(segmentLocation((FileSystemSpooledSegmentHandle) handle)).create();
    }

    @Override
    public FileSystemSpooledSegmentHandle create(SpoolingContext context)
    {
        return FileSystemSpooledSegmentHandle.random(context, ttl);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle segmentHandle = (FileSystemSpooledSegmentHandle) handle;
        checkExpiration(segmentHandle);
        return fileSystem.newInputFile(segmentLocation((FileSystemSpooledSegmentHandle) handle)).newStream();
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
    {
        try {
            fileSystem.deleteFile(segmentLocation((FileSystemSpooledSegmentHandle) handle));
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete segment");
        }
    }

    private static String safeString(String value)
    {
        return value.replaceAll("[^a-zA-Z0-9-_/]", "-");
    }

    private Location segmentLocation(FileSystemSpooledSegmentHandle handle)
    {
        checkExpiration(handle);
        return Location.of(location + "/" + safeString(handle.name()));
    }

    private void checkExpiration(FileSystemSpooledSegmentHandle handle)
    {
        if (handle.validUntil().before(new Date())) {
            throw new RuntimeException("Segment has expired");
        }
    }

    @Override
    public Slice serialize(SpooledSegmentHandle handle)
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        try (DynamicSliceOutput output = new DynamicSliceOutput(64)) {
            output.writeLong(fileHandle.validUntil().getTime());
            output.writeInt(fileHandle.name().length());
            output.writeBytes(utf8Slice(fileHandle.name()));
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SpooledSegmentHandle deserialize(Slice slice)
    {
        try (SliceInput input = slice.getInput()) {
            Date validUntil = new Date(input.readLong());
            int nameLength = input.readInt();
            byte[] name = new byte[nameLength];
            input.readBytes(name);
            return new FileSystemSpooledSegmentHandle(new String(name, UTF_8), validUntil);
        }
    }
}
