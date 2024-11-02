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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper class for
 * <ul>
 * <li> delegate reader page source and
 * <li> columns to be read by the delegate (present only if different from the columns desired by the connector pagesource)
 * </ul>
 */
public class ReaderPageSource
{
    private final ConnectorPageSource connectorPageSource;
    private final Optional<ReaderColumns> columns;

    public ReaderPageSource(ConnectorPageSource connectorPageSource, Optional<ReaderColumns> columns)
    {
        this.connectorPageSource = requireNonNull(connectorPageSource, "connectorPageSource is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    public ConnectorPageSource getUnprojectedPageSource()
    {
        if (columns.isPresent()) {
            throw new IllegalStateException("Projection columns are present");
        }
        return connectorPageSource;
    }

    public ConnectorPageSource getProjectedPageSource(List<? extends ColumnHandle> expectedColumns, ProjectionGetter projectionGetter)
    {
        if (columns.isEmpty()) {
            return connectorPageSource;
        }

        ReaderColumns readColumns = columns.get();
        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        for (int i = 0; i < expectedColumns.size(); i++) {
            transforms.dereferenceField(ImmutableList.<Integer>builder()
                    .add(readColumns.getPositionForColumnAt(i))
                    .addAll(projectionGetter.get(expectedColumns.get(i), readColumns.getForColumnAt(i)))
                    .build());
        }
        return transforms.build(connectorPageSource);
    }

    public interface ProjectionGetter
    {
        List<Integer> get(ColumnHandle required, ColumnHandle read);
    }

    public Optional<ReaderColumns> getReaderColumns()
    {
        return columns;
    }

    public static ReaderPageSource noProjectionAdaptation(ConnectorPageSource connectorPageSource)
    {
        return new ReaderPageSource(connectorPageSource, Optional.empty());
    }
}
