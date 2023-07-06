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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_FUNCTION_INVOCATION;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class FakerMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final List<SchemaInfo> schemas = new ArrayList<>();
    private final FakerConfig config;

    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    private final Map<Long, TableInfo> tables = new HashMap<>();

    @Inject
    public FakerMetadata(FakerConfig config)
    {
        this.schemas.add(new SchemaInfo(SCHEMA_NAME));
        this.config = config;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return schemas.stream().map(SchemaInfo::getName).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (schemas.stream().anyMatch(schema -> schema.getName().equals(schemaName))) {
            throw new TrinoException(ALREADY_EXISTS, format("Schema [%s] already exists", schemaName));
        }
        schemas.add(new SchemaInfo(schemaName, properties));
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        verify(schemas.remove(getSchema(schemaName)));
    }

    private SchemaInfo getSchema(String name)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(s -> s.getName().equals(name))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(NOT_FOUND, format("Schema [%s] does not exist", name));
        }
        return schema.get();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        SchemaInfo schema = getSchema(schemaTableName.getSchemaName());
        Long id = tableIds.get(schemaTableName);
        if (id == null) {
            return null;
        }
        long schemaLimit = (long) schema.getProperties().getOrDefault(SchemaInfo.DEFAULT_LIMIT_PROPERTY, config.getDefaultLimit());
        long tableLimit = (long) tables.get(id).getProperties().getOrDefault(TableInfo.DEFAULT_LIMIT_PROPERTY, schemaLimit);
        tableLimit = (tableLimit + config.getMinSplits() - 1) / config.getMinSplits();
        return new FakerTableHandle(id, schemaTableName, TupleDomain.all(), tableLimit);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = Types.checkType(connectorTableHandle, FakerTableHandle.class, "tableHandle");
        if (tableHandle.getId().isEmpty()) {
            throw new TrinoException(INVALID_TABLE_FUNCTION_INVOCATION, "Table functions are not supported");
        }
        return tables.get(tableHandle.getId().get()).getMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.values().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .map(TableInfo::getSchemaTableName)
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = Types.checkType(connectorTableHandle, FakerTableHandle.class, "tableHandle");
        return tables.get(tableHandle.getId().get())
                .getColumns().stream()
                .collect(toImmutableMap(ColumnInfo::getName, ColumnInfo::getHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        FakerTableHandle tableHandle = Types.checkType(connectorTableHandle, FakerTableHandle.class, "tableHandle");
        if (tableHandle.getId().isEmpty()) {
            throw new TrinoException(INVALID_TABLE_FUNCTION_INVOCATION, "Table functions are not supported");
        }
        return tables.get(tableHandle.getId().get())
                .getColumn(columnHandle)
                .getMetadata();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .map(table -> TableColumnsMetadata.forTable(
                        table.getSchemaTableName(),
                        table.getMetadata().getColumns()))
                .iterator();
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        TableInfo info = tables.remove(handle.getId().get());
        if (info != null) {
            tableIds.remove(info.getSchemaTableName());
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);

        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        long tableId = handle.getId().get();

        TableInfo oldInfo = tables.get(tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldInfo.getColumns(),
                oldInfo.getProperties(),
                oldInfo.getComment()));

        tableIds.remove(oldInfo.getSchemaTableName());
        tableIds.put(newTableName, tableId);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        long tableId = handle.getId().get();

        TableInfo oldInfo = tables.get(tableId);
        Map<String, Object> newProperties = Stream.concat(
                        oldInfo.getProperties().entrySet().stream()
                                .filter(entry -> !properties.containsKey(entry.getKey())),
                        properties.entrySet().stream()
                                .filter(entry -> entry.getValue().isPresent()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        tables.put(tableId, oldInfo.cloneWithProperties(newProperties));
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        long tableId = handle.getId().get();

        TableInfo oldInfo = tables.get(tableId);
        tables.put(tableId, oldInfo.cloneWithComment(comment));
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        long tableId = handle.getId().get();

        TableInfo oldInfo = tables.get(tableId);
        List<ColumnInfo> columns = oldInfo.getColumns().stream()
                .map(columnInfo -> {
                    if (columnInfo.getHandle().equals(column)) {
                        return columnInfo.cloneWithComment(comment);
                    }
                    return columnInfo;
                })
                .collect(Collectors.toUnmodifiableList());
        tables.put(tableId, oldInfo.cloneWithColumns(columns));
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized FakerOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        SchemaInfo schema = getSchema(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long tableId = nextTableId.getAndIncrement();

        double schemaNullProbability = (double) schema.getProperties().getOrDefault(SchemaInfo.NULL_PROBABILITY_PROPERTY, config.getNullProbability());
        double tableNullProbability = (double) tableMetadata.getProperties().getOrDefault(TableInfo.NULL_PROBABILITY_PROPERTY, schemaNullProbability);

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            if (column.getProperties().containsKey(ColumnInfo.GENERATOR_PROPERTY)
                    && !(column.getType() instanceof CharType || column.getType() instanceof VarcharType || column.getType() instanceof VarbinaryType)) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `generator` property can only be set for CHAR, VARCHAR or VARBINARY columns");
            }
            double nullProbability = 0;
            if (column.isNullable()) {
                nullProbability = (double) column.getProperties().getOrDefault(ColumnInfo.NULL_PROBABILITY_PROPERTY, tableNullProbability);
            }
            columns.add(new ColumnInfo(
                    new FakerColumnHandle(
                            i,
                            column.getName(),
                            column.getType(),
                            nullProbability,
                            (String) column.getProperties().get(ColumnInfo.GENERATOR_PROPERTY),
                            null),
                    column));
        }

        tableIds.put(tableMetadata.getTable(), tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columns.build(),
                tableMetadata.getProperties(),
                tableMetadata.getComment()));

        return new FakerOutputTableHandle(tableId, ImmutableSet.copyOf(tableIds.values()));
    }

    private void checkSchemaExists(String schemaName)
    {
        if (schemas.stream().noneMatch(schema -> schema.getName().equals(schemaName))) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private void checkTableNotExists(SchemaTableName tableName)
    {
        if (tableIds.containsKey(tableName)) {
            throw new TrinoException(ALREADY_EXISTS, format("Table [%s] already exists", tableName));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        FakerOutputTableHandle fakerOutputHandle = (FakerOutputTableHandle) tableHandle;

        long tableId = fakerOutputHandle.getTable();

        TableInfo info = tables.get(tableId);
        checkState(
                info != null,
                "Uninitialized tableId [%s.%s]",
                info.getSchemaName(),
                info.getTableName());

        // TODO ensure fragments is empty?

        tables.put(tableId, new TableInfo(tableId, info.getSchemaName(), info.getTableName(), info.getColumns(), info.getProperties(), info.getComment()));
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(s -> s.getName().equals(schemaName))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        return schema.get().getProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;
        if (fakerTable.getLimit() == limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(
                fakerTable.cloneWithLimit(limit),
                false,
                true));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return Optional.empty();
        }
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.getDomains().isEmpty()) {
            throw new IllegalArgumentException("summary cannot be none");
        }

        TupleDomain<ColumnHandle> currentConstraint = fakerTable.getConstraint();
        if (currentConstraint.getDomains().isEmpty()) {
            throw new IllegalArgumentException("currentConstraint is none but should be all!");
        }

        // push down everything, unsupported constraints will throw an exception during data generation
        boolean anyUpdated = false;
        for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().get().entrySet()) {
            FakerColumnHandle column = (FakerColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

            if (currentConstraint.getDomains().get().containsKey(column)) {
                Domain currentDomain = currentConstraint.getDomains().get().get(column);
                // it is important to avoid processing same constraint multiple times
                // so that planner doesn't get stuck in a loop
                if (currentDomain.equals(domain)) {
                    continue;
                }
                // TODO write test cases for this, it doesn't seem to work with IS NULL
                currentDomain.union(domain);
            }
            else {
                Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().get());
                domains.put(column, domain);
                currentConstraint = TupleDomain.withColumnDomains(domains);
            }
            anyUpdated = true;
        }
        if (!anyUpdated) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                fakerTable.cloneWithConstraint(currentConstraint),
                TupleDomain.all(),
                true));
    }
}
