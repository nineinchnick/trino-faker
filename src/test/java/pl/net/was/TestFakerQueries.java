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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

public class TestFakerQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return FakerQueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM faker", "VALUES 'default', 'information_schema'");
        assertUpdate("CREATE TABLE faker.default.test (id INTEGER, name VARCHAR)");
        assertTableColumnNames("faker.default.test", "id", "name");
    }

    @Test
    public void selectFromTable()
    {
        @Language("SQL")
        String tableQuery = "CREATE TABLE faker.default.all_types (" +
                "rnd_bigint bigint NOT NULL, " +
                "rnd_integer integer NOT NULL, " +
                "rnd_smallint smallint NOT NULL, " +
                "rnd_tinyint tinyint NOT NULL, " +
                "rnd_boolean boolean NOT NULL, " +
                "rnd_date date NOT NULL, " +
                "rnd_decimal decimal NOT NULL, " +
                "rnd_real real NOT NULL, " +
                "rnd_double double NOT NULL, " +
                "rnd_interval_day_time interval day to second NOT NULL, " +
                "rnd_interval_year interval year to month NOT NULL, " +
                "rnd_timestamp timestamp NOT NULL, " +
                "rnd_timestamp0 timestamp(0) NOT NULL, " +
                "rnd_timestamp6 timestamp(6) NOT NULL, " +
                "rnd_timestamp9 timestamp(9) NOT NULL, " +
                "rnd_timestamptz timestamp with time zone NOT NULL, " +
                "rnd_timestamptz0 timestamp(0) with time zone NOT NULL, " +
                "rnd_timestamptz6 timestamp(6) with time zone NOT NULL, " +
                "rnd_timestamptz9 timestamp(9) with time zone NOT NULL, " +
                "rnd_time time NOT NULL, " +
                "rnd_time0 time(0) NOT NULL, " +
                "rnd_time6 time(6) NOT NULL, " +
                "rnd_time9 time(9) NOT NULL, " +
                "rnd_timetz time with time zone NOT NULL, " +
                "rnd_timetz0 time(0) with time zone NOT NULL, " +
                "rnd_timetz6 time(6) with time zone NOT NULL, " +
                "rnd_timetz9 time(9) with time zone NOT NULL, " +
                "rnd_varbinary varbinary NOT NULL, " +
                "rnd_varchar varchar NOT NULL, " +
                "rnd_char char NOT NULL, " +
                "rnd_ipaddress ipaddress NOT NULL, " +
                "rnd_uuid uuid NOT NULL" +
                ")";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery = "SELECT " +
                "count(distinct rnd_bigint), " +
                "count(distinct rnd_integer), " +
                "count(rnd_smallint), " +
                "count(rnd_tinyint), " +
                "count(distinct rnd_boolean), " +
                "count(distinct rnd_date), " +
                "count(distinct rnd_decimal), " +
                "count(rnd_real), " +
                "count(distinct rnd_double), " +
                "count(distinct rnd_interval_day_time), " +
                "count(distinct rnd_interval_year), " +
                "count(distinct rnd_timestamp), " +
                "count(distinct rnd_timestamp0), " +
                "count(distinct rnd_timestamp6), " +
                "count(distinct rnd_timestamp9), " +
                "count(distinct rnd_timestamptz), " +
                "count(distinct rnd_timestamptz0), " +
                "count(distinct rnd_timestamptz6), " +
                "count(distinct rnd_timestamptz9), " +
                "count(distinct rnd_time), " +
                "count(distinct rnd_time0), " +
                "count(distinct rnd_time6), " +
                "count(distinct rnd_time9), " +
                "count(distinct rnd_timetz), " +
                "count(distinct rnd_timetz0), " +
                "count(distinct rnd_timetz6), " +
                "count(distinct rnd_timetz9), " +
                "count(distinct rnd_varbinary), " +
                "count(distinct rnd_varchar), " +
                "count(distinct rnd_char), " +
                "count(distinct rnd_ipaddress), " +
                "count(distinct rnd_uuid) " +
                "FROM all_types";
        assertQuery(testQuery,
                "VALUES (1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "2," +
                        "1000," +
                        "1000," +
                        // real, double
                        "1000," +
                        "1000," +
                        // intervals
                        "1000," +
                        "1000," +
                        // timestamps
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        // time
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        "1000," +
                        // varbinary, varchar, char
                        "1000," +
                        "1000," +
                        "19," +
                        // ip address, uuid
                        "1000," +
                        "1000)");
        assertUpdate("DROP TABLE faker.default.all_types");
    }

    @Test
    public void selectLimit()
    {
        @Language("SQL")
        String tableQuery = "CREATE TABLE faker.default.single_column (rnd_bigint bigint NOT NULL)";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery = "SELECT count(rnd_bigint) FROM (SELECT rnd_bigint FROM single_column LIMIT 5) a";
        assertQuery(testQuery, "VALUES (5)");

        testQuery = "SELECT count(distinct rnd_bigint) FROM single_column LIMIT 5";
        assertQuery(testQuery, "VALUES (1000)");
        assertUpdate("DROP TABLE faker.default.single_column");
    }

    @Test
    public void selectDefaultTableLimit()
    {
        @Language("SQL")
        String tableQuery = "CREATE TABLE faker.default.default_table_limit (rnd_bigint bigint NOT NULL) WITH (default_limit = 100)";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery = "SELECT count(distinct rnd_bigint) FROM default_table_limit";
        assertQuery(testQuery, "VALUES (100)");

        assertUpdate("DROP TABLE faker.default.default_table_limit");
    }

    @Test
    public void selectOnlyNulls()
    {
        @Language("SQL")
        String tableQuery = "CREATE TABLE faker.default.only_nulls (rnd_bigint bigint) WITH (null_probability = 1.0)";
        assertUpdate(tableQuery);
        tableQuery = "CREATE TABLE faker.default.only_nulls_column (rnd_bigint bigint WITH (null_probability = 1.0))";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery = "SELECT count(distinct rnd_bigint) FROM only_nulls";
        assertQuery(testQuery, "VALUES (0)");
        testQuery = "SELECT count(distinct rnd_bigint) FROM only_nulls_column";
        assertQuery(testQuery, "VALUES (0)");

        assertUpdate("DROP TABLE faker.default.only_nulls");
        assertUpdate("DROP TABLE faker.default.only_nulls_column");
    }

    @Test
    public void selectGenerator()
    {
        @Language("SQL")
        String tableQuery = "CREATE TABLE faker.default.generators (" +
                "name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'), " +
                "age_years INTEGER NOT NULL" +
                ")";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery = "SELECT count(name) FILTER (WHERE LENGTH(name) - LENGTH(REPLACE(name, ' ', '')) = 1) FROM generators";
        assertQuery(testQuery, "VALUES (1000)");

        assertUpdate("DROP TABLE faker.default.generators");
    }
}
