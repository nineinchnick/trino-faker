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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import net.datafaker.Faker;

import javax.inject.Inject;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.System.arraycopy;
import static java.util.stream.Collectors.toList;

public class FakerRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final FakerConfig config;
    private final FakerMetadata metadata;
    private final Random random;
    private final Faker faker;

    @Inject
    public FakerRecordSetProvider(FakerConfig config, FakerMetadata metadata)
    {
        this.config = config;
        this.metadata = metadata;
        random = new Random();
        faker = new Faker(random);
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

        FakerTableHandle fakerTable = (FakerTableHandle) table;
        Stream<List<Object>> stream = Stream.generate(() -> generateRow(handles)).limit(fakerTable.getLimit());
        Iterable<List<Object>> rows = stream::iterator;

        List<Type> mappedTypes = handles
                .stream()
                .map(FakerColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, rows);
    }

    private List<Object> generateRow(List<FakerColumnHandle> handles)
    {
        return handles.stream().map(this::generateValue).collect(toList());
    }

    private Object generateValue(FakerColumnHandle handle)
    {
        if (handle.getNullProbability() > 0 && random.nextDouble() <= handle.getNullProbability()) {
            return null;
        }
        Type type = handle.getType();
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            return random.nextLong();
        }
        if (INTEGER.equals(type)) {
            return random.nextInt();
        }
        if (SMALLINT.equals(type)) {
            return faker.number().numberBetween(Short.MIN_VALUE, Short.MAX_VALUE);
        }
        if (TINYINT.equals(type)) {
            return faker.number().numberBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        if (BOOLEAN.equals(type)) {
            return random.nextBoolean();
        }
        if (DATE.equals(type)) {
            return random.nextInt();
        }
        if (type instanceof DecimalType) {
            return Int128.valueOf(random.nextLong());
        }
        if (REAL.equals(type)) {
            return floatToRawIntBits(random.nextFloat());
        }
        if (DOUBLE.equals(type)) {
            return random.nextDouble();
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) {
            return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (type instanceof TimeType || type instanceof TimeWithTimeZoneType) {
            return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (type instanceof VarbinaryType) {
            return faker.lorem().sentence(20).getBytes();
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            return varcharType.getLength()
                    .map(length -> faker.lorem().maxLengthSentence(length))
                    .orElse(faker.lorem().sentence(20));
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return faker.lorem().maxLengthSentence(charType.getLength());
        }
        // not supported: ROW, ARRAY, MAP, JSON
        if (IPADDRESS.equals(type)) {
            byte[] address;
            try {
                address = faker.internet().getIpV4Address().getAddress();
            }
            catch (UnknownHostException e) {
                // ignore
                return null;
            }

            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
            return bytes;
        }
        // not supported: GEOMETRY
        if (UUID.equals(type)) {
            java.util.UUID uuid = java.util.UUID.randomUUID();
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }
}
