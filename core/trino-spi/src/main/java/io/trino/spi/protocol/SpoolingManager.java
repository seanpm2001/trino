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
package io.trino.spi.protocol;

import io.airlift.slice.Slice;
import io.trino.spi.Experimental;
import io.trino.spi.protocol.SpooledLocation.DirectLocation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

@Experimental(eta = "2025-05-31")
public interface SpoolingManager
{
    SpooledSegmentHandle create(SpoolingContext context);

    OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException;

    InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException;

    default void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
    }

    default Optional<DirectLocation> directLocation(SpooledSegmentHandle handle, OptionalInt ttlSeconds)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    // Converts the handle to a location that client will be redirected to
    SpooledLocation location(SpooledSegmentHandle handle)
            throws IOException;

    // Converts spooled location back to the handle
    SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers);
}
