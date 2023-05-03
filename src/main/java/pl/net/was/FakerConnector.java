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
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;
import static pl.net.was.FakerTransactionHandle.INSTANCE;

public class FakerConnector
        implements Connector
{
    private final FakerMetadata metadata;
    private final FakerSplitManager splitManager;
    private final FakerPageSourceProvider pageSourceProvider;
    private final FakerPageSinkProvider pageSinkProvider;

    public static final String NULL_PROBABILITY_PROPERTY = "null_probability";
    public static final String DEFAULT_LIMIT_PROPERTY = "default_limit";

    @Inject
    public FakerConnector(
            FakerMetadata metadata,
            FakerSplitManager splitManager,
            FakerPageSourceProvider pageSourceProvider,
            FakerPageSinkProvider pageSinkProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return Set.of(NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        SchemaInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in any column that allows them, in any table of this schema",
                        null,
                        false),
                longProperty(
                        SchemaInfo.DEFAULT_LIMIT_PROPERTY,
                        "Default limit of rows returned from any table in this schema, if not specified in the query",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        TableInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in any column in this table that allows them",
                        null,
                        false),
                longProperty(
                        TableInfo.DEFAULT_LIMIT_PROPERTY,
                        "Default limit of rows returned from this table if not specified in the query",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        ColumnInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in this column, if it allows them",
                        null,
                        false),
                stringProperty(
                        ColumnInfo.GENERATOR_PROPERTY,
                        "Name of the Faker library generator used to generate data for this column",
                        null,
                        pageSourceProvider::validateGenerator,
                        false));
    }
}
