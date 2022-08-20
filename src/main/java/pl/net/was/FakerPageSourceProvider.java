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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import net.datafaker.Faker;

import javax.inject.Inject;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiConsumer;

import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class FakerPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Random random;
    private final Faker faker;

    static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1_000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
            10_000_000_000_000L,
            100_000_000_000_000L,
            1_000_000_000_000_000L,
            10_000_000_000_000_000L,
            100_000_000_000_000_000L,
            1_000_000_000_000_000_000L
    };

    @Inject
    public FakerPageSourceProvider()
    {
        random = new Random();
        faker = new Faker(random);
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        List<FakerColumnHandle> handles = columns
                .stream()
                .map(c -> (FakerColumnHandle) c)
                .collect(toList());

        FakerTableHandle fakerTable = (FakerTableHandle) table;

        return new PageSource(faker, random, handles, fakerTable);
    }

    private static class PageSource
            implements ConnectorPageSource
    {
        private static final int ROWS_PER_REQUEST = 4096;

        private final Random random;
        private final Faker faker;
        private final FakerTableHandle table;
        private final List<BiConsumer<BlockBuilder, Long>> generators;
        private long completedBytes;
        private long completedRows;

        private final PageBuilder pageBuilder;

        private boolean closed;

        private PageSource(Faker faker, Random random, List<FakerColumnHandle> columns, FakerTableHandle table)
        {
            this.faker = requireNonNull(faker, "faker is null");
            this.random = requireNonNull(random, "random is null");
            this.table = requireNonNull(table, "table is null");
            List<Type> types = requireNonNull(columns, "columns is null")
                    .stream()
                    .map(FakerColumnHandle::getType)
                    .collect(toList());
            this.generators = columns
                    .stream()
                    .map(column -> constraintedValueGenerator(
                            column,
                            table.getConstraint().getDomains().get().getOrDefault(column, Domain.all(column.getType()))))
                    .collect(toList());
            // TODO create generators for every column
            this.pageBuilder = new PageBuilder(types);
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return closed && pageBuilder.isEmpty();
        }

        @Override
        public Page getNextPage()
        {
            if (!closed) {
                int positions = (int) Math.min(table.getLimit() - completedRows, ROWS_PER_REQUEST);
                if (positions <= 0) {
                    closed = true;
                }
                else {
                    pageBuilder.declarePositions(positions);
                    for (int column = 0; column < generators.size(); column++) {
                        BlockBuilder output = pageBuilder.getBlockBuilder(column);
                        for (int i = 0; i < positions; i++) {
                            generators.get(column).accept(output, completedRows + i);
                        }
                    }
                    completedRows += positions;
                }
            }

            // only return a page if the buffer is full, or we are finishing
            if ((closed && !pageBuilder.isEmpty()) || pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                completedBytes += page.getSizeInBytes();
                return page;
            }

            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return pageBuilder.getSizeInBytes();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        private BiConsumer<BlockBuilder, Long> constraintedValueGenerator(FakerColumnHandle handle, Domain domain)
        {
            if (handle.getStep().isPresent()) {
                // TODO validate null probability, type and constraint
                Long step = handle.getStep().get();
                long start = (long) domain.getValues().getRanges().getSpan().getLowValue().get();
                return (output, rowNum) -> BIGINT.writeLong(output, start + step * rowNum);
            }
            if (domain.isSingleValue()) {
                BiConsumer<BlockBuilder, Object> singleValueWriter = objectWriter(handle.getType());
                return (output, rowNum) -> singleValueWriter.accept(output, domain.getSingleValue());
            }
            if (domain.getValues().isDiscreteSet()) {
                List<Object> values = domain.getValues().getDiscreteSet();
                if (domain.getValues().getDiscreteValues().isInclusive()) {
                    BiConsumer<BlockBuilder, Object> singleValueWriter = objectWriter(handle.getType());
                    return (output, rowNum) -> singleValueWriter.accept(output, values.get(random.nextInt(values.size())));
                }
                throw new TrinoException(INVALID_ROW_FILTER, "Generating random values for an exclusive discrete set is not supported");
            }
            if (domain.getValues().getRanges().getRangeCount() > 1) {
                // this would require calculating weights for each range to retain uniform distribution
                throw new TrinoException(INVALID_ROW_FILTER, "Generating random values from more than one range is not supported");
            }
            BiConsumer<BlockBuilder, Long> generator = randomValueGenerator(handle, domain.getValues().getRanges().getSpan());
            if (handle.getNullProbability() == 0) {
                return generator;
            }
            return (output, rowNum) -> {
                if (random.nextDouble() <= handle.getNullProbability()) {
                    output.appendNull();
                }
                else {
                    generator.accept(output, rowNum);
                }
            };
        }

        private BiConsumer<BlockBuilder, Long> randomValueGenerator(FakerColumnHandle handle, Range range)
        {
            Optional<String> generator = handle.getGenerator();
            if (generator.isPresent()) {
                if (!range.isAll()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Predicates for columns with a generator expression are not supported");
                }
                return (output, rowNum) -> VARCHAR.writeSlice(output, Slices.utf8Slice(faker.expression(generator.get())));
            }
            Type type = handle.getType();
            // check every type in order defined in StandardTypes
            if (BIGINT.equals(type)) {
                return (output, rowNum) -> BIGINT.writeLong(output, generateLong(range, 1));
            }
            if (INTEGER.equals(type)) {
                return (output, rowNum) -> INTEGER.writeLong(output, generateInt(range));
            }
            if (SMALLINT.equals(type)) {
                return (output, rowNum) -> SMALLINT.writeLong(output, generateShort(range));
            }
            if (TINYINT.equals(type)) {
                return (output, rowNum) -> TINYINT.writeLong(output, generateTiny(range));
            }
            if (BOOLEAN.equals(type)) {
                if (!range.isAll()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Range or not a single value predicates for boolean columns are not supported");
                }
                return (output, rowNum) -> BOOLEAN.writeBoolean(output, random.nextBoolean());
            }
            if (DATE.equals(type)) {
                return (output, rowNum) -> DATE.writeLong(output, generateInt(range));
            }
            if (type instanceof DecimalType decimalType) {
                return decimalGenerator(range, decimalType);
            }
            if (REAL.equals(type)) {
                return (output, rowNum) -> REAL.writeLong(output, floatToRawIntBits(generateFloat(range)));
            }
            if (DOUBLE.equals(type)) {
                return (output, rowNum) -> DOUBLE.writeDouble(output, generateDouble(range));
            }
            // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
            if (INTERVAL_DAY_TIME.equals(type)) {
                return (output, rowNum) -> INTERVAL_DAY_TIME.writeLong(output, generateLong(range, 1));
            }
            if (INTERVAL_YEAR_MONTH.equals(type)) {
                return (output, rowNum) -> INTERVAL_YEAR_MONTH.writeLong(output, generateInt(range));
            }
            if (type instanceof TimestampType) {
                return timestampGenerator(range, (TimestampType) type);
            }
            if (type instanceof TimestampWithTimeZoneType) {
                return timestampWithTimeZoneGenerator(range, (TimestampWithTimeZoneType) type);
            }
            if (type instanceof TimeType timeType) {
                long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
                return (output, rowNum) -> timeType.writeLong(output, generateLongDefaults(range, factor, 0, PICOSECONDS_PER_DAY));
            }
            if (type instanceof TimeWithTimeZoneType) {
                return timeWithTimeZoneGenerator(range, (TimeWithTimeZoneType) type);
            }
            if (type instanceof VarbinaryType varType) {
                if (!range.isAll()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varbinary columns are not supported");
                }
                return (output, rowNum) -> varType.writeSlice(output, Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38))));
            }
            if (type instanceof VarcharType varcharType) {
                if (!range.isAll()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varchar columns are not supported");
                }
                if (varcharType.getLength().isPresent()) {
                    int length = varcharType.getLength().get();
                    return (output, rowNum) -> varcharType.writeSlice(output, Slices.utf8Slice(faker.lorem().maxLengthSentence(random.nextInt(length))));
                }
                return (output, rowNum) -> varcharType.writeSlice(output, Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38))));
            }
            if (type instanceof CharType charType) {
                if (!range.isAll()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Predicates for char columns are not supported");
                }
                return (output, rowNum) -> charType.writeSlice(output, Slices.utf8Slice(faker.lorem().maxLengthSentence(charType.getLength())));
            }
            // not supported: ROW, ARRAY, MAP, JSON
            if (IPADDRESS.equals(type)) {
                return generateIpV4(range);
            }
            // not supported: GEOMETRY
            if (UUID.equals(type)) {
                return generateUUID(range);
            }

            throw new IllegalArgumentException("Unsupported type " + type);
        }

        private BiConsumer<BlockBuilder, Object> objectWriter(Type type)
        {
            // check every type in order defined in StandardTypes
            if (BIGINT.equals(type)) {
                return (output, value) -> BIGINT.writeLong(output, (Long) value);
            }
            if (INTEGER.equals(type)) {
                return (output, value) -> INTEGER.writeLong(output, (Long) value);
            }
            if (SMALLINT.equals(type)) {
                return (output, value) -> SMALLINT.writeLong(output, (Long) value);
            }
            if (TINYINT.equals(type)) {
                return (output, value) -> TINYINT.writeLong(output, (Long) value);
            }
            if (BOOLEAN.equals(type)) {
                return (output, value) -> BOOLEAN.writeBoolean(output, (Boolean) value);
            }
            if (DATE.equals(type)) {
                return (output, value) -> DATE.writeLong(output, (Long) value);
            }
            if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    return (output, value) -> decimalType.writeLong(output, (Long) value);
                }
                else {
                    return decimalType::writeObject;
                }
            }
            if (REAL.equals(type) || DOUBLE.equals(type)) {
                return (output, value) -> REAL.writeDouble(output, (Double) value);
            }
            // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
            if (INTERVAL_DAY_TIME.equals(type)) {
                return (output, value) -> INTERVAL_DAY_TIME.writeLong(output, (Long) value);
            }
            if (INTERVAL_YEAR_MONTH.equals(type)) {
                return (output, value) -> INTERVAL_YEAR_MONTH.writeLong(output, (Long) value);
            }
            if (type instanceof TimestampType tzType) {
                if (tzType.isShort()) {
                    return (output, value) -> tzType.writeLong(output, (Long) value);
                }
                else {
                    return tzType::writeObject;
                }
            }
            if (type instanceof TimestampWithTimeZoneType tzType) {
                if (tzType.isShort()) {
                    return (output, value) -> tzType.writeLong(output, (Long) value);
                }
                else {
                    return tzType::writeObject;
                }
            }
            if (type instanceof TimeType timeType) {
                return (output, value) -> timeType.writeLong(output, (Long) value);
            }
            if (type instanceof TimeWithTimeZoneType tzType) {
                if (tzType.isShort()) {
                    return (output, value) -> tzType.writeLong(output, (Long) value);
                }
                else {
                    return tzType::writeObject;
                }
            }
            if (type instanceof VarbinaryType varType) {
                return (output, value) -> varType.writeSlice(output, (Slice) value);
            }
            if (type instanceof VarcharType varType) {
                return (output, value) -> varType.writeSlice(output, (Slice) value);
            }
            if (type instanceof CharType charType) {
                return (output, value) -> charType.writeSlice(output, (Slice) value);
            }
            // not supported: ROW, ARRAY, MAP, JSON
            if (IPADDRESS.equals(type)) {
                return (output, value) -> IPADDRESS.writeSlice(output, (Slice) value);
            }
            // not supported: GEOMETRY
            if (UUID.equals(type)) {
                return (output, value) -> UUID.writeSlice(output, (Slice) value);
            }

            throw new IllegalArgumentException("Unsupported type " + type);
        }

        private long generateLong(Range range, long factor)
        {
            return generateLongDefaults(range, factor, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        private long generateLongDefaults(Range range, long factor, long min, long max)
        {
            return faker.number().numberBetween(
                    roundDiv((long) range.getLowValue().orElse(min), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    // TODO does the inclusion only apply to positive numbers?
                    roundDiv((long) range.getHighValue().orElse(max), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
        }

        private int generateInt(Range range)
        {
            return (int) faker.number().numberBetween(
                    (long) range.getLowValue().orElse((long) Integer.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    (long) range.getHighValue().orElse((long) Integer.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
        }

        private short generateShort(Range range)
        {
            return (short) faker.number().numberBetween(
                    (long) range.getLowValue().orElse((long) Short.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    (long) range.getHighValue().orElse((long) Short.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
        }

        private byte generateTiny(Range range)
        {
            return (byte) faker.number().numberBetween(
                    (long) range.getLowValue().orElse((long) Byte.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    (long) range.getHighValue().orElse((long) Byte.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
        }

        private float generateFloat(Range range)
        {
            // TODO normalize ranges in applyFilter, so they always have bounds
            float minValue = range.getLowValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                minValue = Math.nextUp(minValue);
            }
            float maxValue = range.getHighValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MAX_VALUE);
            if (!range.isHighUnbounded() && !range.isHighInclusive()) {
                maxValue = Math.nextDown(maxValue);
            }
            return minValue + (maxValue - minValue) * random.nextFloat();
        }

        private double generateDouble(Range range)
        {
            double minValue = (double) range.getLowValue().orElse(Double.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                minValue = Math.nextUp(minValue);
            }
            double maxValue = (double) range.getHighValue().orElse(Double.MAX_VALUE);
            if (!range.isHighUnbounded() && !range.isHighInclusive()) {
                maxValue = Math.nextDown(maxValue);
            }
            return minValue + (maxValue - minValue) * random.nextDouble();
        }

        private BiConsumer<BlockBuilder, Long> decimalGenerator(Range range, DecimalType decimalType)
        {
            if (decimalType.isShort()) {
                long min = -999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()];
                long max = 999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()];
                return (output, rowNum) -> decimalType.writeLong(output, generateLongDefaults(range, 1, min, max));
            }
            Int128 low = (Int128) range.getLowValue().orElse(Decimals.MIN_UNSCALED_DECIMAL);
            Int128 high = (Int128) range.getHighValue().orElse(Decimals.MAX_UNSCALED_DECIMAL);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                long[] result = new long[2];
                Int128Math.add(low.getHigh(), low.getLow(), 0, 1, result, 0);
                low = Int128.valueOf(result);
            }
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                long[] result = new long[2];
                Int128Math.add(high.getHigh(), high.getLow(), 0, 1, result, 0);
                high = Int128.valueOf(result);
            }

            BigInteger currentRange = BigInteger.valueOf(Long.MAX_VALUE);
            BigInteger desiredRange = high.toBigInteger().subtract(low.toBigInteger());
            Int128 finalLow = low;
            return (output, rowNum) -> decimalType.writeObject(output, Int128.valueOf(
                    new BigInteger(63, random).multiply(desiredRange).divide(currentRange).add(finalLow.toBigInteger())));
        }

        private BiConsumer<BlockBuilder, Long> timestampGenerator(Range range, TimestampType tzType)
        {
            if (tzType.isShort()) {
                long factor = POWERS_OF_TEN[6 - tzType.getPrecision()];
                return (output, rowNum) -> tzType.writeLong(output, generateLong(range, factor));
            }
            LongTimestamp low = (LongTimestamp) range.getLowValue()
                    .orElse(new LongTimestamp(Long.MIN_VALUE, 0));
            LongTimestamp high = (LongTimestamp) range.getHighValue()
                    .orElse(new LongTimestamp(Long.MAX_VALUE, PICOSECONDS_PER_MICROSECOND - 1));
            int factor;
            if (tzType.getPrecision() <= 6) {
                factor = (int) POWERS_OF_TEN[6 - tzType.getPrecision()];
                low = new LongTimestamp(
                        roundDiv(low.getEpochMicros(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                        0);
                high = new LongTimestamp(
                        roundDiv(high.getEpochMicros(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0),
                        0);
            }
            else {
                factor = (int) POWERS_OF_TEN[12 - tzType.getPrecision()];
                int lowPicosOfMicro = roundDiv(low.getPicosOfMicro(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
                low = new LongTimestamp(
                        low.getEpochMicros() - (lowPicosOfMicro < 0 ? 1 : 0),
                        (lowPicosOfMicro + factor) % factor);
                int highPicosOfMicro = roundDiv(high.getPicosOfMicro(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
                high = new LongTimestamp(
                        high.getEpochMicros() + (highPicosOfMicro > factor ? 1 : 0),
                        highPicosOfMicro % factor);
            }
            LongTimestamp finalLow = low;
            LongTimestamp finalHigh = high;
            return (output, rowNum) -> {
                long epochMicros = faker.number().numberBetween(finalLow.getEpochMicros(), finalHigh.getEpochMicros());
                if (tzType.getPrecision() <= 6) {
                    epochMicros *= factor;
                    tzType.writeObject(output, new LongTimestamp(epochMicros * factor, 0));
                    return;
                }
                int picosOfMicro;
                if (epochMicros == finalLow.getEpochMicros()) {
                    picosOfMicro = faker.number().numberBetween(
                            finalLow.getPicosOfMicro(),
                            finalLow.getEpochMicros() == finalHigh.getEpochMicros() ?
                                    finalHigh.getPicosOfMicro()
                                    : (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
                }
                else if (epochMicros == finalHigh.getEpochMicros()) {
                    picosOfMicro = faker.number().numberBetween(0, finalHigh.getPicosOfMicro());
                }
                else {
                    picosOfMicro = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
                }
                tzType.writeObject(output, new LongTimestamp(epochMicros, picosOfMicro * factor));
            };
        }

        private BiConsumer<BlockBuilder, Long> timestampWithTimeZoneGenerator(Range range, TimestampWithTimeZoneType tzType)
        {
            if (tzType.isShort()) {
                TimeZoneKey defaultTZ = range.getLowValue()
                        .map(v -> unpackZoneKey((long) v))
                        .orElse(range.getHighValue()
                                .map(v -> unpackZoneKey((long) v))
                                .orElse(TimeZoneKey.UTC_KEY));
                long factor = POWERS_OF_TEN[3 - tzType.getPrecision()];
                return (output, rowNum) -> {
                    long millis = faker.number().numberBetween(
                            roundDiv(unpackMillisUtc((long) range.getLowValue().orElse(Long.MIN_VALUE)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                            roundDiv(unpackMillisUtc((long) range.getHighValue().orElse(Long.MAX_VALUE)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
                    tzType.writeLong(output, packDateTimeWithZone(millis, defaultTZ));
                };
            }
            short defaultTZ = range.getLowValue()
                    .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                    .orElse(range.getHighValue()
                            .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                            .orElse(TimeZoneKey.UTC_KEY.getKey()));
            LongTimestampWithTimeZone low = (LongTimestampWithTimeZone) range.getLowValue()
                    .orElse(fromEpochMillisAndFraction(Long.MIN_VALUE >> 12, 0, defaultTZ));
            LongTimestampWithTimeZone high = (LongTimestampWithTimeZone) range.getHighValue()
                    .orElse(fromEpochMillisAndFraction(Long.MAX_VALUE >> 12, PICOSECONDS_PER_MILLISECOND - 1, defaultTZ));
            if (low.getTimeZoneKey() != high.getTimeZoneKey()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for timestamp with time zone columns must have the same time zone");
            }
            int factor = (int) POWERS_OF_TEN[12 - tzType.getPrecision()];
            int lowPicosOfMilli = roundDiv(low.getPicosOfMilli(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            low = fromEpochMillisAndFraction(
                    low.getEpochMillis() - (lowPicosOfMilli < 0 ? 1 : 0),
                    (lowPicosOfMilli + factor) % factor,
                    low.getTimeZoneKey());
            int highPicosOfMilli = roundDiv(high.getPicosOfMilli(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            high = fromEpochMillisAndFraction(
                    high.getEpochMillis() + (highPicosOfMilli > factor ? 1 : 0),
                    highPicosOfMilli % factor,
                    high.getTimeZoneKey());
            LongTimestampWithTimeZone finalLow = low;
            LongTimestampWithTimeZone finalHigh = high;
            return (output, rowNum) -> {
                long millis = faker.number().numberBetween(finalLow.getEpochMillis(), finalHigh.getEpochMillis());
                int picosOfMilli;
                if (millis == finalLow.getEpochMillis()) {
                    picosOfMilli = faker.number().numberBetween(
                            finalLow.getPicosOfMilli(),
                            finalLow.getEpochMillis() == finalHigh.getEpochMillis() ?
                                    finalHigh.getPicosOfMilli()
                                    : (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
                }
                else if (millis == finalHigh.getEpochMillis()) {
                    picosOfMilli = faker.number().numberBetween(0, finalHigh.getPicosOfMilli());
                }
                else {
                    picosOfMilli = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
                }
                tzType.writeObject(output, fromEpochMillisAndFraction(millis, picosOfMilli * factor, defaultTZ));
            };
        }

        private BiConsumer<BlockBuilder, Long> timeWithTimeZoneGenerator(Range range, TimeWithTimeZoneType timeType)
        {
            if (timeType.isShort()) {
                int offsetMinutes = range.getLowValue()
                        .map(v -> unpackOffsetMinutes((long) v))
                        .orElse(range.getHighValue()
                                .map(v -> unpackOffsetMinutes((long) v))
                                .orElse(0));
                long factor = POWERS_OF_TEN[9 - timeType.getPrecision()];
                long low = roundDiv(range.getLowValue().map(v -> unpackTimeNanos((long) v)).orElse(0L), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
                long high = roundDiv(range.getHighValue().map(v -> unpackTimeNanos((long) v)).orElse(NANOSECONDS_PER_DAY), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
                return (output, rowNum) -> {
                    long nanos = faker.number().numberBetween(low, high) * factor;
                    timeType.writeLong(output, packTimeWithTimeZone(nanos, offsetMinutes));
                };
            }
            int offsetMinutes = range.getLowValue()
                    .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                    .orElse(range.getHighValue()
                            .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                            .orElse(0));
            LongTimeWithTimeZone low = (LongTimeWithTimeZone) range.getLowValue()
                    .orElse(new LongTimeWithTimeZone(0, offsetMinutes));
            LongTimeWithTimeZone high = (LongTimeWithTimeZone) range.getHighValue()
                    .orElse(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY, offsetMinutes));
            if (low.getOffsetMinutes() != high.getOffsetMinutes()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for time with time zone columns must have the same time zone");
            }
            int factor = (int) POWERS_OF_TEN[12 - timeType.getPrecision()];
            long longLow = roundDiv(low.getPicoseconds(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long longHigh = roundDiv(high.getPicoseconds(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return (output, rowNum) -> {
                long picoseconds = faker.number().numberBetween(longLow, longHigh) * factor;
                timeType.writeObject(output, new LongTimeWithTimeZone(picoseconds, offsetMinutes));
            };
        }

        private BiConsumer<BlockBuilder, Long> generateIpV4(Range range)
        {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for ipaddress columns are not supported");
            }
            return (output, rowNum) -> {
                byte[] address;
                try {
                    address = Inet4Address.getByAddress(new byte[] {
                            (byte) (random.nextInt(254) + 2),
                            (byte) (random.nextInt(254) + 2),
                            (byte) (random.nextInt(254) + 2),
                            (byte) (random.nextInt(254) + 2)}).getAddress();
                }
                catch (UnknownHostException e) {
                    // ignore
                    output.appendNull();
                    return;
                }

                byte[] bytes = new byte[16];
                bytes[10] = (byte) 0xff;
                bytes[11] = (byte) 0xff;
                arraycopy(address, 0, bytes, 12, 4);

                IPADDRESS.writeSlice(output, Slices.wrappedBuffer(bytes, 0, 16));
            };
        }

        private BiConsumer<BlockBuilder, Long> generateUUID(Range range)
        {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for uuid columns are not supported");
            }
            return (output, rowNum) -> {
                java.util.UUID uuid = java.util.UUID.randomUUID();
                ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
                bb.putLong(uuid.getMostSignificantBits());
                bb.putLong(uuid.getLeastSignificantBits());
                UUID.writeSlice(output, Slices.wrappedBuffer(bb.array(), 0, 16));
            };
        }

        @SuppressWarnings("NumericCastThatLosesPrecision")
        public static int roundDiv(int value, long factor)
        {
            return (int) roundDiv((long) value, factor);
        }

        public static long roundDiv(long value, long factor)
        {
            if (factor <= 0) {
                throw new IllegalArgumentException("Factor must be > 0");
            }

            if (factor == 1) {
                return value;
            }

            if (value >= 0) {
                long rounded = value + (factor / 2);
                if (rounded < 0) {
                    rounded = Long.MAX_VALUE;
                }
                return rounded / factor;
            }

            long rounded = value + 1 - (factor / 2);
            if (rounded >= 0) {
                rounded = Long.MIN_VALUE;
            }
            return rounded / factor;
        }
    }

    public void validateGenerator(String generator)
    {
        faker.expression(generator);
    }
}
