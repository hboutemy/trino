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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.HostAddress;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@Path("/v1/spooled/segments/{segmentHandle}")
@ResourceSecurity(PUBLIC)
public class SegmentResource
{
    private final SpoolingManagerBridge spoolingManager;
    private final boolean useWorkers;
    private final InternalNodeManager nodeManager;
    private final AtomicLong nextWorkerIndex = new AtomicLong();
    private final boolean directStorageAccess;

    @Inject
    public SegmentResource(SpoolingManagerBridge spoolingManager, SpoolingConfig config, InternalNodeManager nodeManager)
    {
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.useWorkers = config.isUseWorkers() && nodeManager.getCurrentNode().isCoordinator();
        this.directStorageAccess = config.isDirectStorageAccess();
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @ResourceSecurity(PUBLIC)
    @Path("")
    public Response download(@Context UriInfo uriInfo, @PathParam("segmentHandle") String segmentHandle)
            throws IOException
    {
        if (directStorageAccess) {
            Optional<URI> location = spoolingManager.directLocation(segmentHandle);
            if (location.isPresent()) {
                return Response.seeOther(location.get()).build();
            }
        }

        if (useWorkers) {
            HostAddress hostAddress = nextActiveNode();
            return Response.seeOther(uriInfo
                    .getRequestUriBuilder()
                        .host(hostAddress.getHostText())
                        .port(hostAddress.getPort())
                        .build())
                    .build();
        }
        return Response.ok(spoolingManager.openInputStream(segmentHandle)).build();
    }

    @DELETE
    @ResourceSecurity(PUBLIC)
    @Path("")
    public Response acknowledge(@PathParam("segmentHandle") String segmentHandle)
    {
        try {
            spoolingManager.drop(segmentHandle);
            return Response.ok().build();
        }
        catch (IOException e) {
            return Response.serverError().build();
        }
    }

    public static UriBuilder spooledSegmentUriBuilder(ExternalUriInfo info)
    {
        return UriBuilder.fromUri(info.baseUriBuilder().build())
                .path(SegmentResource.class)
                .path(SegmentResource.class, "download");
    }

    public HostAddress nextActiveNode()
    {
        List<InternalNode> internalNodes = ImmutableList.copyOf(nodeManager.getActiveNodesSnapshot().getAllNodes());
        return internalNodes.get(toIntExact(nextWorkerIndex.incrementAndGet() % internalNodes.size()))
                .getHostAndPort();
    }
}
