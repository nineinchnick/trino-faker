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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
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
import java.util.stream.Stream;

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
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.stream.Collectors.toList;

public class FakerRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final FakerConfig config;
    private final FakerMetadata metadata;
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
        Stream<List<Object>> stream = Stream.generate(() -> generateRow(handles, fakerTable.getConstraint())).limit(fakerTable.getLimit());
        Iterable<List<Object>> rows = stream::iterator;

        List<Type> mappedTypes = handles
                .stream()
                .map(FakerColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, rows);
    }

    private List<Object> generateRow(List<FakerColumnHandle> handles, TupleDomain<ColumnHandle> summary)
    {
        return handles.stream().map(handle -> generateConstrainedValue(handle, summary.getDomains().get().getOrDefault(handle, Domain.all(handle.getType())))).collect(toList());
    }

    private Object generateConstrainedValue(FakerColumnHandle handle, Domain domain)
    {
        if (domain.isNullableSingleValue()) {
            return domain.getNullableSingleValue();
        }
        if (domain.getValues().isDiscreteSet()) {
            List<Object> values = domain.getValues().getDiscreteSet();
            if (domain.getValues().getDiscreteValues().isInclusive()) {
                return values.get(random.nextInt(values.size()));
            }
            Object value;
            do {
                value = generateValue(handle, Range.all(handle.getType()));
            }
            while (values.contains(value));
            return value;
        }
        if (domain.getValues().getRanges().getRangeCount() > 1) {
            // this would require calculating weights for each range to retain uniform distribution
            throw new TrinoException(INVALID_ROW_FILTER, "Generating random values from more than one range is not supported");
        }
        return generateValue(handle, domain.getValues().getRanges().getSpan());
    }

    private Object generateValue(FakerColumnHandle handle, Range range)
    {
        if (handle.getNullProbability() > 0 && random.nextDouble() <= handle.getNullProbability()) {
            return null;
        }
        Optional<String> generator = handle.getGenerator();
        if (generator.isPresent()) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for columns with a generator expression are not supported");
            }
            return faker.expression(generator.get());
        }
        if (range.isSingleValue()) {
            return range.getSingleValue();
        }
        Type type = handle.getType();
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            return generateLong(range, 1);
        }
        if (INTEGER.equals(type)) {
            return generateInt(range);
        }
        if (SMALLINT.equals(type)) {
            return generateShort(range);
        }
        if (TINYINT.equals(type)) {
            return generateTiny(range);
        }
        if (BOOLEAN.equals(type)) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range or not a single value predicates for boolean columns are not supported");
            }
            return random.nextBoolean();
        }
        if (DATE.equals(type)) {
            return generateInt(range);
        }
        if (type instanceof DecimalType) {
            return generateDecimal(range, (DecimalType) type);
        }
        if (REAL.equals(type)) {
            return floatToRawIntBits(generateFloat(range));
        }
        if (DOUBLE.equals(type)) {
            return generateDouble(range);
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            return generateLong(range, 1);
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return generateInt(range);
        }
        if (type instanceof TimestampType) {
            return generateTimestamp(range, (TimestampType) type);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return generateTimestampWithTimeZone(range, (TimestampWithTimeZoneType) type);
        }
        if (type instanceof TimeType) {
            TimeType timeType = (TimeType) type;
            return generateLongDefaults(range, POWERS_OF_TEN[12 - timeType.getPrecision()], 0, PICOSECONDS_PER_DAY);
        }
        if (type instanceof TimeWithTimeZoneType) {
            return generateTimeWithTimeZone(range, (TimeWithTimeZoneType) type);
        }
        if (type instanceof VarbinaryType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varbinary columns are not supported");
            }
            return faker.lorem().sentence(3 + random.nextInt(38)).getBytes();
        }
        if (type instanceof VarcharType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varchar columns are not supported");
            }
            VarcharType varcharType = (VarcharType) type;
            return varcharType.getLength()
                    .map(length -> faker.lorem().maxLengthSentence(random.nextInt(length)))
                    .orElse(faker.lorem().sentence(3 + random.nextInt(38)));
        }
        if (type instanceof CharType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for char columns are not supported");
            }
            CharType charType = (CharType) type;
            return faker.lorem().maxLengthSentence(charType.getLength());
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

    private Object generateDecimal(Range range, DecimalType decimalType)
    {
        if (decimalType.isShort()) {
            return generateLongDefaults(
                    range,
                    1,
                    -999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()],
                    999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()]);
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

        BigInteger randomNumber = new BigInteger(63, random);
        BigInteger currentRange = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger desiredRange = high.toBigInteger().subtract(low.toBigInteger());
        return Int128.valueOf(randomNumber.multiply(desiredRange).divide(currentRange).add(low.toBigInteger()));
    }

    private Object generateTimestamp(Range range, TimestampType tzType)
    {
        if (tzType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return generateLong(range, POWERS_OF_TEN[6 - tzType.getPrecision()]);
        }
        LongTimestamp low = (LongTimestamp) range.getLowValue()
                .orElse(new LongTimestamp(Long.MIN_VALUE, PICOSECONDS_PER_MICROSECOND - 1));
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
        long epochMicros = faker.number().numberBetween(low.getEpochMicros(), high.getEpochMicros());
        int picosOfMicro = 0;
        if (tzType.getPrecision() <= 6) {
            epochMicros *= factor;
        }
        else {
            if (epochMicros == low.getEpochMicros()) {
                picosOfMicro = faker.number().numberBetween(
                        low.getPicosOfMicro(),
                        low.getEpochMicros() == high.getEpochMicros() ?
                                high.getPicosOfMicro()
                                : (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            else if (epochMicros == high.getEpochMicros()) {
                picosOfMicro = faker.number().numberBetween(0, high.getPicosOfMicro());
            }
            else {
                picosOfMicro = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            picosOfMicro *= factor;
        }
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private Object generateTimestampWithTimeZone(Range range, TimestampWithTimeZoneType tzType)
    {
        if (tzType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            TimeZoneKey defaultTZ = range.getLowValue()
                    .map(v -> unpackZoneKey((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackZoneKey((long) v))
                            .orElse(TimeZoneKey.UTC_KEY));
            long factor = POWERS_OF_TEN[3 - tzType.getPrecision()];
            long millis = faker.number().numberBetween(
                    roundDiv(unpackMillisUtc((long) range.getLowValue().orElse(Long.MIN_VALUE)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    roundDiv(unpackMillisUtc((long) range.getHighValue().orElse(Long.MAX_VALUE)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
            return packDateTimeWithZone(millis, defaultTZ);
        }
        short defaultTZ = range.getLowValue()
                .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                .orElse(range.getHighValue()
                        .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                        .orElse(TimeZoneKey.UTC_KEY.getKey()));
        LongTimestampWithTimeZone low = (LongTimestampWithTimeZone) range.getLowValue()
                .orElse(fromEpochMillisAndFraction(Long.MIN_VALUE >> 12, PICOSECONDS_PER_MILLISECOND - 1, defaultTZ));
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
        long millis = faker.number().numberBetween(low.getEpochMillis(), high.getEpochMillis());
        int picosOfMilli;
        if (millis == low.getEpochMillis()) {
            picosOfMilli = faker.number().numberBetween(
                    low.getPicosOfMilli(),
                    low.getEpochMillis() == high.getEpochMillis() ?
                            high.getPicosOfMilli()
                            : (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
        }
        else if (millis == high.getEpochMillis()) {
            picosOfMilli = faker.number().numberBetween(0, high.getPicosOfMilli());
        }
        else {
            picosOfMilli = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
        }
        return fromEpochMillisAndFraction(millis, picosOfMilli * factor, defaultTZ);
    }

    private Object generateTimeWithTimeZone(Range range, TimeWithTimeZoneType timeType)
    {
        if (timeType.getPrecision() <= TimeWithTimeZoneType.MAX_SHORT_PRECISION) {
            int offsetMinutes = range.getLowValue()
                    .map(v -> unpackOffsetMinutes((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackOffsetMinutes((long) v))
                            .orElse(0));
            long factor = POWERS_OF_TEN[9 - timeType.getPrecision()];
            long nanos = faker.number().numberBetween(
                    roundDiv(unpackTimeNanos((long) range.getLowValue().orElse(0L)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    roundDiv(unpackTimeNanos((long) range.getHighValue().orElse(NANOSECONDS_PER_DAY)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
            return packTimeWithTimeZone(nanos, offsetMinutes);
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
        long picoseconds = faker.number().numberBetween(
                roundDiv(low.getPicoseconds(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                roundDiv(high.getPicoseconds(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
        return new LongTimeWithTimeZone(picoseconds, offsetMinutes);
    }

    private byte[] generateIpV4(Range range)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for ipaddress columns are not supported");
        }
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
            return null;
        }

        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xff;
        bytes[11] = (byte) 0xff;
        arraycopy(address, 0, bytes, 12, 4);
        return bytes;
    }

    private byte[] generateUUID(Range range)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for uuid columns are not supported");
        }
        java.util.UUID uuid = java.util.UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
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

        return (value + 1 - (factor / 2)) / factor;
    }

    public void validateGenerator(String generator)
    {
        faker.expression(generator);
    }
}
