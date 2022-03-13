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
import com.google.common.collect.Iterables;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toList;

public class FakerRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final FakerConfig config;
    private final FakerMetadata metadata;

    @Inject
    public FakerRecordSetProvider(FakerConfig config, FakerMetadata metadata)
    {
        this.config = config;
        this.metadata = metadata;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        List<FakerColumnHandle> handles = columns
                .stream()
                .map(c -> (FakerColumnHandle) c)
                .collect(toList());

        Stream<List<Object>> stream = Stream.generate(() -> generateRow(handles)).limit(1000L);
        Iterable<List<Object>> rows = stream::iterator;

        List<Type> mappedTypes = handles
                .stream()
                .map(FakerColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, rows);
    }

    private List<Object> generateRow(List<FakerColumnHandle> handles)
    {
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        handles.forEach(handle -> {
                    values.add(generateValue(handle));
                });
        return values.build();
    }

    private Object generateValue(FakerColumnHandle handle)
    {
        Type type = handle.getType();
        if (VARCHAR.equals(type)) {
            return "string";
        }
        if (BIGINT.equals(type) || INTEGER.equals(type)) {
            return 1L;
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }
}
