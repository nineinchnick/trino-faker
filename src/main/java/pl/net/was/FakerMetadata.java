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

package pl.net.was;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class FakerMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    public static final Map<String, List<ColumnMetadata>> columns = new ImmutableMap.Builder<String, List<ColumnMetadata>>()
            .put("single_row", ImmutableList.of(
                    new ColumnMetadata("id", VARCHAR),
                    new ColumnMetadata("type", VARCHAR),
                    new ColumnMetadata("name", VARCHAR)))
            .build();

    @Inject
    public FakerMetadata()
    {
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getSchemaName().equals(SCHEMA_NAME)) {
            return null;
        }
        return new FakerTableHandle(
                schemaTableName,
                TupleDomain.none(),
                0,
                Integer.MAX_VALUE,
                1,
                null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = Types.checkType(connectorTableHandle, FakerTableHandle.class, "tableHandle");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        return new ConnectorTableMetadata(
                schemaTableName,
                columns.get(schemaTableName.getTableName()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return columns
                .keySet()
                .stream()
                .map(table -> new SchemaTableName(SCHEMA_NAME, table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorSession, connectorTableHandle).getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> new FakerColumnHandle(column.getName(), column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        FakerColumnHandle restColumnHandle = Types.checkType(columnHandle, FakerColumnHandle.class, "columnHandle");
        return new ColumnMetadata(restColumnHandle.getName(), restColumnHandle.getType());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession connectorSession,
            SchemaTablePrefix schemaTablePrefix)
    {
        return columns.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> new SchemaTableName(schemaTablePrefix.getSchema().orElse(""), e.getKey()),
                        Map.Entry::getValue));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = Types.checkType(connectorTableHandle, FakerTableHandle.class, "tableHandle");
        return new FakerInsertTableHandle(tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}
